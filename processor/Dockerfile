FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transform.py processor.py ./

ENV PYTHONUNBUFFERED=1 \
    PORT=8080

RUN useradd --create-home appuser
USER appuser

EXPOSE 8080
CMD ["python", "processor.py"]
