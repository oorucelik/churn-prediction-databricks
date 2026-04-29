FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fetch_tmdb.py .
RUN mkdir -p /app/seeds
CMD ["python", "fetch_tmdb.py"]
