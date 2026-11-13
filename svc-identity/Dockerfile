# syntax=docker/dockerfile:1.7
# Build from the repo root: docker build -f svc-identity/Dockerfile -t svc-identity .
FROM golang:1.26 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY lumo-contracts/go.mod lumo-contracts/go.sum* lumo-contracts/
COPY lumo-kit/go.mod lumo-kit/go.sum* lumo-kit/
COPY svc-identity/go.mod svc-identity/go.sum* svc-identity/
RUN --mount=type=cache,target=/go/pkg/mod cd svc-identity && go mod download
COPY lumo-contracts lumo-contracts
COPY lumo-kit lumo-kit
COPY svc-identity svc-identity
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd svc-identity && go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/identity

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["serve"]
