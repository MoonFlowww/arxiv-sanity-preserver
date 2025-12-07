FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    git \
    wget \
    curl \
    build-essential \
    poppler-utils \
    imagemagick \
    ghostscript \
    libfreetype6-dev \
    libjpeg-dev \
    zlib1g-dev \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# python deps
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# copy code
COPY . .

# arxiv sanity default port
EXPOSE 5000

CMD ["python3", "serve.py"]

