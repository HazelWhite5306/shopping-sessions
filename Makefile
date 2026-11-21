# Local workflow. DEV is the generated 1M-event dev set, ANSWER the unzipped dev-answer.zip.
DEV ?= ../dev
ANSWER ?= ../answer
OUT ?= ../out

build:
	cargo build --release

test:
	cargo test --release

check-dev: build
	rm -rf $(OUT) && DATA_DIR=$(DEV) OUTPUT_DIR=$(OUT) ./target/release/sessions
	python3 tools/check.py $(OUT) $(ANSWER)

docker:
	docker build --platform linux/amd64 -t sessions .

.PHONY: build test check-dev docker

fixtures: build
	python3 tools/run_fixtures.py fixtures -- ./target/release/sessions

.PHONY: fixtures
