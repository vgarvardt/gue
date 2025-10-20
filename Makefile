export CGO_ENABLED=0

.PHONY: lint
lint:
	golangci-lint run --config=./.github/linters/.golangci.yml --fix

.PHONY: spell-lint
spell-lint:
	docker run \
		--interactive --tty --rm \
		--volume "$(CURDIR):/workdir" \
		--workdir "/workdir" \
		python:3.14-slim bash -c "python -m pip install --upgrade pip && pip install 'codespell>=2.4.1' && codespell"

.PHONY: docs-lint
docs-lint:
	docker run \
		--interactive --tty --rm \
		--volume "$(CURDIR):/workdir" \
		--workdir "/workdir" \
		node:lts-alpine sh -c 'npm install --global --production --update-notifier=false markdownlint-cli@0.45.0 && markdownlint *.md -c .github/linters/.markdown-lint.yml'

.PHONY: deps-up
deps-up:
	docker compose up --detach --wait
	docker compose port postgres 5432

.PHONY: deps-down
deps-down:
	docker compose down --volumes --remove-orphans

.PHONY: .test
.test:
	PG_HOST=$$(docker compose port postgres 5432) && \
	TEST_POSTGRES="postgres://test:test@$$PG_HOST/test?sslmode=disable" \
	go test -timeout 2m -cover -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: test
test: deps-up .test deps-down
