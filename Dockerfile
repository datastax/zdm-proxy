##########
# NOTE: When building this image, there is an assumption that you are in the top level directory of the repository.
# $ docker build . -f ./Dockerfile -t zdm-proxy
##########

FROM golang:1.23-bookworm AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

COPY go.mod .
COPY go.sum .
COPY proxy ./proxy
COPY antlr ./antlr
RUN ls

# Build the application
RUN go build -o main ./proxy

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from /build to /dist
RUN cp /build/main .

# Build a small image
FROM alpine

COPY --from=builder /dist/main /
COPY LICENSE /

ENV ZDM_PROXY_LISTEN_ADDRESS="0.0.0.0"
ENV ZDM_METRICS_ADDRESS="0.0.0.0"

# Command to run
ENTRYPOINT ["/main"]
