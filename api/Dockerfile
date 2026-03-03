FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    openssh-client \
    tinysshd ucspi-tcp \
    netcat-openbsd \
    dos2unix \
    ffmpeg \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir poetry && \
    pip install uvicorn[standard]

COPY pyproject.toml ./

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

COPY . .

RUN poetry install --no-interaction --no-ansi

# Configure tinysshd
RUN tinysshd-makekey /etc/tinyssh/sshkeydir

# Create mpiuser and .ssh directory for SSH keys
RUN useradd -m -s /bin/bash mpiuser
RUN mkdir -p /home/mpiuser/.ssh
COPY ssh/* /home/mpiuser/.ssh/
RUN chmod 700 /home/mpiuser/.ssh/
RUN chmod 600 /home/mpiuser/.ssh/authorized_keys
RUN chmod 600 /home/mpiuser/.ssh/id_ecdsa

# Copy and setup entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
