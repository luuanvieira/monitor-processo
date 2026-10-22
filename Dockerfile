FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /monitor ./cmd/monitor

# Imagem final mínima: só o binário, sem shell, rodando sem root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /monitor /monitor
EXPOSE 8080
ENTRYPOINT ["/monitor"]
