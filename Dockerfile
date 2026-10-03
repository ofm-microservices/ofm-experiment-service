FROM golang:1.25.11 AS build
WORKDIR /src
COPY . /src
RUN go mod download && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/load-test-service ./cmd/load-test-service && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/capacity-controller ./cmd/capacity-controller
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/load-test-service /load-test-service
COPY --from=build /out/capacity-controller /capacity-controller
USER nonroot:nonroot
ENTRYPOINT ["/load-test-service"]
