FROM golang:1.14 AS build

COPY . /src/project
WORKDIR /src/project

RUN export CGO_ENABLED=0 &&\
    export GOPROXY=https://goproxy.io &&\
    make &&\
    cp cmd/router/router /router &&\
    cp etc/router.sample.conf /router.conf

FROM debian:10
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=build /router /router
COPY --from=build /router.conf /etc/router.conf

EXPOSE 8002

CMD ["/router", "-c", "/etc/router.conf"]
