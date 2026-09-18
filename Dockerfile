##########
# NOTE: When building this image, there is an assumption that you are in the top level directory of the repository.
# $ docker build . -f ./Dockerfile -t zdm-proxy
#
# For a multi-arch build, for example:
# $ docker buildx build \
#     --platform linux/amd64,linux/arm64 \
#     -f ./Dockerfile \
#     -t zdm-proxy \
#     .
##########

# Build on the architecture of the build host rather than the target
# architecture. This allows Go to cross-compile the binary without
# requiring QEMU/emulation for the builder stage.
FROM --platform=$BUILDPLATFORM golang:1.26.5-bookworm AS builder

# TARGETOS and TARGETARCH are automatically provided by BuildKit/buildx
# for each platform requested via --platform.
ARG TARGETOS
ARG TARGETARCH

ENV GO111MODULE=on \
    CGO_ENABLED=0

# Move to working directory /build
WORKDIR /build

COPY go.mod .
COPY go.sum .
COPY proxy ./proxy
COPY antlr ./antlr
RUN ls

# Build the application.
# Cross-compile it for the target platform instead of hard-coding amd64.
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o main ./proxy

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from /build to /dist
RUN cp /build/main .

# Build a small image.
# The final image uses the target architecture selected by --platform.
FROM alpine

COPY --from=builder /dist/main /
COPY LICENSE /

ENV ZDM_PROXY_LISTEN_ADDRESS="0.0.0.0"
ENV ZDM_METRICS_ADDRESS="0.0.0.0"

# Command to run
ENTRYPOINT ["/main"]
