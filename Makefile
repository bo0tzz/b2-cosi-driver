IMAGE ?= ghcr.io/bo0tzz/b2-cosi-driver
TAG   ?= latest

.PHONY: build test lint docker-build docker-push

build:
	go build ./...

test:
	go test ./...

lint:
	go vet ./...

docker-build:
	docker build -t $(IMAGE):$(TAG) .

docker-push:
	docker push $(IMAGE):$(TAG)
