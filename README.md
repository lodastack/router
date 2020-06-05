
# Router [![CircleCI](https://circleci.com/gh/lodastack/router.svg?style=svg)](https://circleci.com/gh/lodastack/router)

NSQ consumer, write points into influxDB cluster

## Build

    make build
    
## Start router
    
    ./router -c router.conf

## Use docker image

    docker run -d -p8002:8002 lodastack/router
