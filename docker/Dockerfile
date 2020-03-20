FROM golang:1.13.8-buster as builder

# For loading data to TiDB
WORKDIR /go/src/github.com/pingcap/
RUN git clone https://github.com/pingcap/go-ycsb.git && \
    cd go-ycsb && \
    make

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

COPY --from=builder /go/src/github.com/pingcap/go-ycsb/bin/go-ycsb /go/bin/go-ycsb

ENTRYPOINT ["/bin/bash"]
