FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# deps del sistema (git solo si realmente instalas algo desde git; aquí no lo necesitas)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copia solo metadata primero para cache
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

# Instala deps del proyecto (PEP 621) sin "editable"
RUN pip install --upgrade pip && \
    pip install --no-cache-dir .

# Copia el código
COPY . /app

ENV PORT=8080
EXPOSE 8080

CMD ["bash", "-lc", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
