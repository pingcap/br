FROM golang:1.13.8-buster

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    vim \
    less \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /go/src/github.com/pingcap/br
COPY . .

# For loading data to TiDB
RUN git clone https://github.com/pingcap/go-ycsb.git /go/src/github.com/pingcap/go-ycsb

ENTRYPOINT ["/bin/bash"]
