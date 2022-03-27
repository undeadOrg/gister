FROM golang:alpine as base

# All these steps will be cached
RUN mkdir /app
WORKDIR /app
COPY go.mod .
COPY go.sum .

# Get dependancies - will also be cached if we won't change mod/sum
RUN go mod download
# COPY the source code as the last step
COPY . .

FROM base as test

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -v ./...


FROM base as build

ARG BUILD_NUM=dev

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 && \
  go build -o /go/bin/gister -ldflags "-X main.buildNum=$BUILD_NUM" *.go


FROM alpine:3 as release

COPY --from=build /go/bin/gister /usr/bin/gister

CMD ["gister"]
