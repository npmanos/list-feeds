# --- Builder Stage ---
# This stage uses the official Go image to build the application binaries.
FROM golang:1.24-alpine AS builder

# Set the working directory inside the container
WORKDIR /app
RUN go env -w GOMODCACHE=/root/.cache/go-build

# Copy go.mod and go.sum files to leverage Docker's layer caching.
# This step only re-runs if the dependencies change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

# Copy the rest of the application's source code
COPY . .

# Build the main server binary.
# The CGO_ENABLED=0 flag creates a statically linked binary, which is ideal for minimal containers.
# The -ldflags="-s -w" strips debugging information, making the binary smaller.
# TARGETOS and TARGETARCH are automatically provided by docker buildx.
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /bin/list-feeds ./cmd/list-feeds

# Build the publisher utility binary as well.
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /bin/publish-feed ./cmd/publish-feed


# --- Final Stage ---
# This stage uses a minimal "distroless" image which contains only our application
# and its direct runtime dependencies. It's more secure than a full OS.
FROM gcr.io/distroless/static-debian12

# Copy the compiled binaries from the builder stage
COPY --from=builder /bin/list-feeds /
COPY --from=builder /bin/publish-feed /

# Copy the sample configuration and any other necessary data files.
# The user will mount a volume to /data to provide their actual config and database.
COPY ./data/config-sample.yml /data/config-sample.yml

# Set the default command to run when the container starts.
# It will look for the config file at /data/config.yml by default.
CMD ["/list-feeds"]