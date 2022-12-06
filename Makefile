.PHONY: build build-docker install publish

build:
	cargo build

build-docker:
	docker build -t netrunner .

install:
	cargo install --path .

publish:
	cargo publish
