all: build

fmt:
	gofmt -l -w -s */

build: fmt 
	cd cmd/router && go build -v -mod=vendor

install: fmt
	cd cmd/router && go install

clean:
	cd cmd/router && go clean
