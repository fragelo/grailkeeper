FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -r requirements.txt

COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY static/ ./static/

EXPOSE 8000
CMD ["python", "backend/entrypoint.py"]
