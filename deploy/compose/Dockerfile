FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# BuildKit supplies these platform arguments for the requested target. Keep
# them declaration-only: defaults here would silently overwrite an explicit
# `docker build --platform` selection.
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/trpc-service ./cmd/trpc-service
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w' -o /out/trpc-healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot

COPY --chown=nonroot:nonroot --from=build /out/trpc-service /trpc-service
COPY --chown=nonroot:nonroot --from=build /out/trpc-healthcheck /trpc-healthcheck
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/trpc-healthcheck", "-path", "/readyz"]
ENTRYPOINT ["/trpc-service"]
CMD ["webui-local"]
