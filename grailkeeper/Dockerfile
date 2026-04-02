FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install dtctl
RUN curl -L https://github.com/dynatrace-oss/dtctl/releases/download/v0.16.0/dtctl_0.16.0_linux_amd64.tar.gz \
    | tar -xz -C /usr/local/bin && chmod +x /usr/local/bin/dtctl

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -r requirements.txt

COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY static/ ./static/

EXPOSE 8000
CMD ["python", "backend/entrypoint.py"]
