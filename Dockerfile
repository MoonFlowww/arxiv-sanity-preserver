FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
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

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# copy code
COPY . .
RUN cargo build --release -p arxiv_sanity_pipeline

# arxiv sanity default port
EXPOSE 5000

CMD ["./target/release/arxiv_sanity_pipeline", "serve"]
