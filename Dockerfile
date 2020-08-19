# Start with a golang image
FROM golang:1.13-stretch as build

ENV GO111MODULE on
ENV GIN_MODE release

# Create a user to run the app as
RUN useradd --shell /bin/bash groove

# Set the workdir to the application path
WORKDIR $GOPATH/src

# Copy all application files
COPY . ./

# Build the app
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 && go build -a -installsuffix nocgo -ldflags="-w -s" -o /go/bin/groove

# Start from a scratch container for a nice and small image
FROM alpine:3.8

# Install ca-certificates for calling https endpoints
RUN apk add --no-cache ca-certificates && mkdir /lib64 && ln -s /lib/libc.musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

# Copy the binary build
COPY --from=build /go/bin/groove /go/bin/groove

# Copy the password file (with the client user) from the build container
COPY --from=build /etc/passwd /etc/passwd

# Set the user to the previously created user
USER groove

# Set the workdir
WORKDIR /go/bin

# Expose the API port
EXPOSE 4600

CMD [ "/go/bin/groove" ]