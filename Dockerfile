# One build stage compiles every binary; one small final stage per service.
# docker compose builds each target, and BuildKit runs the shared stage once.
FROM golang:1.24-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

# No separate "go mod download" layer: it would fetch every module in
# go.mod, including the test-only ones (testcontainers and the Docker
# client). go build downloads only what the binaries import.
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Distroless: no shell, no package manager, a non-root user. Health checks
# run the binary itself with a "healthcheck" argument.
FROM gcr.io/distroless/static-debian12:nonroot AS ledger
COPY --from=build /out/ledger /ledger
USER nonroot:nonroot
EXPOSE 50051 9101
ENTRYPOINT ["/ledger"]

FROM gcr.io/distroless/static-debian12:nonroot AS transfers
COPY --from=build /out/transfers /transfers
USER nonroot:nonroot
EXPOSE 8080 9100
ENTRYPOINT ["/transfers"]

FROM gcr.io/distroless/static-debian12:nonroot AS notifications
COPY --from=build /out/notifications /notifications
USER nonroot:nonroot
EXPOSE 8081 9102
ENTRYPOINT ["/notifications"]

FROM gcr.io/distroless/static-debian12:nonroot AS fakerisk
COPY --from=build /out/fakerisk /fakerisk
USER nonroot:nonroot
EXPOSE 8090
ENTRYPOINT ["/fakerisk"]
