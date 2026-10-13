FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /terry .

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /terry /terry
EXPOSE 3000
ENTRYPOINT ["/terry"]
