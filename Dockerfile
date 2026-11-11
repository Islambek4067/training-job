FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -o /out/soupq ./cmd/soupq

FROM alpine:3.20
COPY --from=build /out/soupq /usr/local/bin/soupq
ENTRYPOINT ["/usr/local/bin/soupq"]
