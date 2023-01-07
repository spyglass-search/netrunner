.PHONY: build build-docker install publish

build:
	cargo build

build-docker:
	docker buildx build --platform=linux/amd64 -t netrunner .

install:
	cargo install --path .

publish:
	cargo publish
