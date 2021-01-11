FROM golang:1.13.8-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    openssl \
    lsof \
    psmisc \
    vim \
    less \
    default-mysql-client \
    python-pip

RUN pip install s3cmd flask

WORKDIR /br

ENTRYPOINT ["/bin/bash"]
