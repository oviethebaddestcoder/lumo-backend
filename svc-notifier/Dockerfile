# syntax=docker/dockerfile:1.7
# Build from the repo root: docker build -f svc-notifier/Dockerfile -t svc-notifier .
FROM golang:1.26 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY lumo-contracts/go.mod lumo-contracts/go.sum* lumo-contracts/
COPY lumo-kit/go.mod lumo-kit/go.sum* lumo-kit/
COPY svc-notifier/go.mod svc-notifier/go.sum* svc-notifier/
RUN --mount=type=cache,target=/go/pkg/mod cd svc-notifier && go mod download
COPY lumo-contracts lumo-contracts
COPY lumo-kit lumo-kit
COPY svc-notifier svc-notifier
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    cd svc-notifier && go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/notifier

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
CMD ["serve"]
