FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /b2-cosi-driver ./cmd/b2-cosi-driver

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /b2-cosi-driver /b2-cosi-driver
ENTRYPOINT ["/b2-cosi-driver"]
