FROM ubuntu:24.04

RUN DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
  # apt-utils \
  bc \
  build-essential \
  sudo \
  nano \
  pkgconf \
  cmake \
  m4 \
  autoconf \
  automake \
  jq \
  python3 \
  python3-dateutil \
  apt-file \
  curl \
  less \
  libopenmpi-dev \
  tinysshd \
  ucspi-tcp \
  openssh-client \
  openssh-server \
  openmpi-bin \
  iputils-ping \
  dnsutils \
  rsync \
  libopenmpi-dev \
  libopencv-dev \
  ffmpeg \
  dos2unix \
  librabbitmq-dev \
  libcurl4-openssl-dev \
  libcjson-dev \
  pkg-config \
  netcat-openbsd
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /root

# configure tinysshd
RUN tinysshd-makekey /etc/tinyssh/sshkeydir
# RUN mkdir .ssh/
COPY ssh/* .ssh/
RUN chmod 700 .ssh/
RUN chmod 600 .ssh/authorized_keys
RUN chmod 600 .ssh/id_ecdsa

# logs
RUN mkdir /var/log/mpi_jobs && chmod 755 /var/log/mpi_jobs
RUN touch /var/log/rabbitmq_consumer.log && chmod 644 /var/log/rabbitmq_consumer.log

# core source code
COPY project/ project
RUN make -C project

# master scripts
COPY bin-master/* /usr/local/bin/

RUN chmod +x project/init.sh

CMD ["/bin/bash", "-c", "/root/project/init.sh", "&&", "tcpserver", "-HRDl0", "0.0.0.0", "22", "/usr/sbin/tinysshd", "-v", "/etc/tinyssh/sshkeydir"]
