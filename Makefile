all: build

fmt:
	gofmt -l -w -s */

build: fmt 
	export GO111MODULE="on"
	export GOPROXY="https://goproxy.io,direct"
	cd cmd/router && go build -v -mod=vendor

install: fmt
	cd cmd/router && go install

clean:
	cd cmd/router && go clean
