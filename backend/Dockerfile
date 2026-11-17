# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod tidy

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 app
COPY --from=build /out/server /server
USER app
EXPOSE 8080
ENTRYPOINT ["/server"]
