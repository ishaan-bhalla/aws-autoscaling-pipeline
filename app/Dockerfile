FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go mod download && CGO_ENABLED=0 go build -o /worker ./cmd/worker && CGO_ENABLED=0 go build -o /createtable ./cmd/createTable

FROM alpine:3.20
COPY --from=build /worker /createtable /usr/local/bin/
ENTRYPOINT ["worker"]
