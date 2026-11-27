FROM python:3.12-slim

WORKDIR /app

# Dependencies first, so this layer is cached when only the code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Print logs immediately (Cloud Logging / docker logs)
ENV PYTHONUNBUFFERED=1 \
    PORT=8080

# Run as a non-root user
RUN useradd --create-home appuser
USER appuser

EXPOSE 8080
CMD ["python", "producer.py"]
