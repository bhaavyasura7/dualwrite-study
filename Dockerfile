# One image definition for every Go component: --build-arg CMD=<dir under cmd/>.
FROM golang:1.27.3-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG CMD
RUN CGO_ENABLED=0 go build -o /app ./cmd/${CMD}

FROM alpine:3.24.4
COPY --from=build /app /app
ENTRYPOINT ["/app"]
