# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/o.oruccelik@gmail.com/tmdb-churn-prediction/notebooks/requirements.txt",
# ]
# ///
# MAGIC %md
# MAGIC # Churn Prediction — XGBoost Training Pipeline
# MAGIC
# MAGIC **Source table:** `prod.dbo_marts.ml_churn_feature_store`
# MAGIC
# MAGIC **Pipeline:**
# MAGIC 1. Load feature store from Unity Catalog
# MAGIC 2. Exploratory sanity checks
# MAGIC 3. Feature preprocessing (impute nulls, select features)
# MAGIC 4. Train/test split (stratified)
# MAGIC 5. Train XGBoost with cross-validation
# MAGIC 6. Evaluate (AUC, precision, recall, feature importance)
# MAGIC 7. Log experiment + model to MLflow
# MAGIC 8. Register model in Unity Catalog

# COMMAND ----------

## 0. Config

CATALOG   = "prod"
SCHEMA    = "dbo_marts"
TABLE     = "ml_churn_feature_store"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

EXPERIMENT_NAME = "/Shared/churn-prediction"
MODEL_NAME      = f"{CATALOG}.dbo_marts.churn_prediction"   # Unity Catalog model path

# Random state — pin for reproducibility
RANDOM_STATE = 42

# COMMAND ----------

## 1. Load Feature Store

import mlflow 
import mlflow.xgboost
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    roc_auc_score, classification_report, confusion_matrix,
    precision_recall_curve, average_precision_score
)
from sklearn.preprocessing import LabelEncoder
import matplotlib.pyplot as plt

# Load from Unity Catalog via Spark → pandas
sdf = spark.table(FULL_TABLE)
df  = sdf.toPandas()

print(f"Loaded {len(df):,} rows × {df.shape[1]} columns from {FULL_TABLE}")
print(f"Churn rate: {df['target_is_churned'].mean():.1%}")
df.head(3)

# COMMAND ----------

## 2. Sanity Checks

# Class balance
print("=== Class distribution ===")
print(df["target_is_churned"].value_counts())
print(df["target_is_churned"].value_counts(normalize=True).map("{:.1%}".format))

# Null rates — flag anything >20%
null_pct = df.isnull().mean().sort_values(ascending=False)
high_null = null_pct[null_pct > 0.20]
if len(high_null):
    print("\n⚠️  Columns with >20% nulls:")
    print(high_null)
else:
    print("\n✅ No columns with >20% nulls")

# COMMAND ----------

## 3. Feature Selection & Preprocessing

# ── Keys and leaky columns to drop ───────────────────────────────────────────
# target_churn_date would leak: it's only set for churned customers
DROP_COLS = [
    "customer_key",
    "customer_id",
    "target_churn_date",       # leakage: only populated for churned
    "target_is_churned",       # this IS the target
]

TARGET = "target_is_churned"

# ── Feature groups (for later importance analysis) ────────────────────────────
FEATURE_GROUPS = {
    "demographic":    ["tenure_days", "age", "age_band_encoded", "plan_type_encoded",
                       "monthly_revenue", "acquisition_channel_encoded", "device_type_encoded"],
    "watch_behavior": ["total_watch_events", "total_active_days", "unique_titles_watched",
                       "unique_sessions", "total_watch_minutes", "avg_watch_duration_min",
                       "max_watch_duration_min", "avg_completion_pct", "completed_count",
                       "abandoned_count", "completion_rate", "abandonment_rate",
                       "resumed_count", "resume_rate", "binge_session_count",
                       "device_count", "days_since_last_watch"],
    "recent_activity":["watch_events_last_7d", "watch_minutes_last_7d",
                       "watch_events_last_30d", "watch_minutes_last_30d",
                       "watch_trend_7d_vs_30d"],
    "subscription":   ["total_subscription_events", "upgrade_count", "downgrade_count",
                       "net_plan_changes", "has_downgraded", "activity_level_encoded"],
    "support":        ["total_support_tickets", "avg_resolution_hours", "avg_sentiment_score",
                       "min_sentiment_score", "negative_ticket_count", "negative_ticket_rate",
                       "unresolved_ticket_count", "billing_tickets", "technical_tickets",
                       "critical_tickets"],
    "ratings":        ["total_ratings", "avg_rating_given", "low_ratings_count",
                       "high_ratings_count"],
    "watchlist":      ["total_watchlist_items", "watchlist_watched_count",
                       "watchlist_conversion_rate"],
    "composite_risk": ["recency_risk_score", "support_frustration_score",
                       "heuristic_churn_risk_score"],
}

FEATURES = [col for group in FEATURE_GROUPS.values() for col in group]
# Only keep features that actually exist in the loaded table
FEATURES = [f for f in FEATURES if f in df.columns]

X = df[FEATURES].copy()
y = df[TARGET].astype(int)

# ── Coerce Spark Decimal → float (toPandas() converts DecimalType to Python
#    Decimal objects, which pandas stores as 'object' dtype) ───────────────────
obj_cols = X.select_dtypes(include=["object"]).columns.tolist()
if obj_cols:
    X[obj_cols] = X[obj_cols].apply(pd.to_numeric, errors="coerce")

# ── Impute remaining nulls with median (XGBoost handles NaN natively too,
#    but explicit imputation makes the pipeline more portable) 
null_cols = X.columns[X.isnull().any()].tolist()
if null_cols:
    X[null_cols] = X[null_cols].fillna(X[null_cols].median())

print(f"\nFeature matrix: {X.shape[0]:,} rows × {X.shape[1]} features")
print(f"Target: {y.sum():,} churned / {(y==0).sum():,} active")

# COMMAND ----------

## 4. Train / Test Split

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.20,
    random_state=RANDOM_STATE,
    stratify=y,       # preserve churn ratio in both splits
)

print(f"Train: {len(X_train):,} rows  |  Test: {len(X_test):,} rows")
print(f"Train churn rate: {y_train.mean():.1%}  |  Test churn rate: {y_test.mean():.1%}")

# COMMAND ----------

## 5. Train XGBoost with Cross-Validation

# Churn datasets are typically imbalanced — weight the positive class
scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
print(f"scale_pos_weight: {scale_pos_weight:.2f}  (handles class imbalance)")

PARAMS = {
    "n_estimators":      300,
    "max_depth":         4,
    "learning_rate":     0.05,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "min_child_weight":  5,
    "gamma":             1,
    "reg_alpha":         0.1,
    "reg_lambda":        1.0,
    "scale_pos_weight":  scale_pos_weight,
    "objective":         "binary:logistic",
    "eval_metric":       "auc",
    "random_state":      RANDOM_STATE,
    "n_jobs":            -1,
}

model = xgb.XGBClassifier(**PARAMS)

# 5-fold stratified CV on training set
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc", error_score='raise', verbose=0)

print(f"\nCV AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
print(f"Fold scores: {[f'{s:.4f}' for s in cv_scores]}")

# Final fit on full training set
model.fit(
    X_train, y_train,
    eval_set=[(X_test, y_test)],
    verbose=False,
)

# COMMAND ----------

## 6. Evaluation

y_pred_proba = model.predict_proba(X_test)[:, 1]
y_pred       = (y_pred_proba >= 0.5).astype(int)

test_auc = roc_auc_score(y_test, y_pred_proba)
avg_precision = average_precision_score(y_test, y_pred_proba)

print(f"=== Test Set Results ===")
print(f"ROC-AUC:           {test_auc:.4f}")
print(f"Avg Precision:     {avg_precision:.4f}")
print(f"\nClassification Report (threshold=0.5):")
print(classification_report(y_test, y_pred, target_names=["Active", "Churned"]))

# Confusion matrix
cm = confusion_matrix(y_test, y_pred)
print(f"Confusion Matrix:\n{cm}")

# COMMAND ----------

### Feature Importance

importance_df = pd.DataFrame({
    "feature":    FEATURES,
    "importance": model.feature_importances_,
}).sort_values("importance", ascending=False)

print("=== Top 15 Features ===")
display(importance_df.head(15))

# Plot
fig, ax = plt.subplots(figsize=(10, 7))
top15 = importance_df.head(15)
ax.barh(top15["feature"][::-1], top15["importance"][::-1], color="#4C72B0")
ax.set_xlabel("Feature Importance (Gain)")
ax.set_title("XGBoost — Top 15 Churn Predictors")
plt.tight_layout()
plt.show()

# COMMAND ----------

## 7. Log to MLflow
mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name="xgboost-churn-v1") as run:
    # Log hyperparameters
    mlflow.log_params(PARAMS)

    # Log CV + test metrics
    mlflow.log_metrics({
        "cv_auc_mean":     cv_scores.mean(),
        "cv_auc_std":      cv_scores.std(),
        "test_auc":        test_auc,
        "test_avg_precision": avg_precision,
        "train_size":      len(X_train),
        "test_size":       len(X_test),
        "n_features":      len(FEATURES),
        "churn_rate":      float(y.mean()),
    })

    # Log feature importance as artifact
    importance_df.to_csv("/tmp/feature_importance.csv", index=False)
    mlflow.log_artifact("/tmp/feature_importance.csv")

    # Log model
    mlflow.xgboost.log_model(
        model,
        artifact_path="model",
        input_example=X_test.head(5),
        registered_model_name=MODEL_NAME,
    )
    run_id = run.info.run_id
    print(f"✅ MLflow run logged: {run_id}")
    print(f"   Experiment: {EXPERIMENT_NAME}")
    print(f"   Model registered as: {MODEL_NAME}")

# COMMAND ----------

## 8. Inference Preview

#Score the full dataset — useful for inspecting high-risk customers.

df["churn_probability"] = model.predict_proba(X)[:, 1]
df["churn_predicted"]   = (df["churn_probability"] >= 0.5).astype(int)
df["risk_tier"] = pd.cut(
    df["churn_probability"],
    bins=[0, 0.3, 0.6, 1.0],
    labels=["low", "medium", "high"],
)

print("=== Risk Tier Distribution ===")
print(df["risk_tier"].value_counts().sort_index())

# Top 10 highest-risk customers
high_risk_cols = [
    "customer_id", "churn_probability", "risk_tier",
    "days_since_last_watch", "has_downgraded",
    "heuristic_churn_risk_score", "negative_ticket_count",
]
display(
    df[high_risk_cols]
    .sort_values("churn_probability", ascending=False)
    .head(10)
)
