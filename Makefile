srcs = src docs/modules/ROOT/examples tests setup.py
isort = isort --quiet \
		--skip docs/modules/ROOT/examples/app_types \
		--skip docs/modules/ROOT/examples/logging/tutorial001.py \
		$(srcs)
black = black --skip-string-normalization $(srcs)

## all: Run linter and tests.
.PHONY: all
all: lint test

## help: Show this help.
.PHONY: help
help: Makefile
	@sed -n 's/^##\s//p' $<

## install: Install all requirements.
.PHONY: install
install: install-corva-sdk install-test install-lint

## install-corva-sdk: Install corva-sdk requirements.
.PHONY: install-corva-sdk
install-corva-sdk:
	@pip install -U pip
	@pip install -U -e .

## install-test: Install test requirements.
.PHONY: install-test
install-test: install-corva-sdk
	@pip install -U -r requirements-test.txt

## install-lint: Install lint requirements.
.PHONY: install-lint
install-lint: install-test
	@pip install -U -r requirements-lint.txt

## test: Run tests.
.PHONY: test
test: up-cache unit-tests integration-tests down-cache

## unit-tests: Run unit tests.
.PHONY: unit-tests
unit-tests: test_path = tests/unit
unit-tests:
	@coverage run -m pytest $(test_path)

## integration-tests: Run integration tests.
.PHONY: integration-tests
integration-tests: export CACHE_URL = redis://localhost:6379
integration-tests: test_path = tests/integration
integration-tests:
	@coverage run -m pytest $(test_path)

## coverage: Display code coverage in the console.
.PHONY: coverage
coverage: test
	@coverage combine
	@coverage report --sort=cover

## coverage-html: Display code coverage in the browser.
.PHONY: coverage-html
coverage-html: test
	@coverage combine
	@coverage html
	@x-www-browser htmlcov/index.html

## lint: Run linter.
.PHONY: lint
lint:
	@flake8 $(srcs)
	@$(black) --check
	@mypy --check-untyped-defs $(srcs)

## format: Format all files.
.PHONY: format
format:
	@$(isort) --force-single-line-imports
	@autoflake --remove-all-unused-imports --recursive --remove-unused-variables \
		--in-place $(srcs)
	@$(black) --quiet
	@$(isort)

## docs: Generate docs.
.PHONY: docs
docs:
	@docker run --rm --tty -v $(CURDIR):/antora antora/antora:3.0.1 \
	docs/antora-playbook.yml

## docs-view: Show HTML docs.
.PHONY: docs-view
docs-view: docs
	@x-www-browser docs/build/site/index.html

## clean: Clean autogenerated files.
.PHONY: clean
clean:
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('*.py[co]'): p.unlink()"
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('__pycache__'): p.rmdir()"
	@-rm -rf src/*.egg-info
	@-rm -rf .pytest_cache
	@-rm -rf htmlcov
	@-rm .coverage*
	@-sudo rm -rf docs/build
	@-rm -rf .mypy_cache

## release: How to release a new version.
.PHONY: release
release:
	@echo "Checkout the master branch."
	@echo "Update src/version.py with new version."
	@echo "Update docs/antora.yml 'version' with new version like '1.6.0'."
	@echo "Update docs/antora-playbook.yml 'content.sources.tags'."
	@echo "Update CHANGELOG.md."
	@echo "Commit the changes."
	@echo "Create tag like 'v1.6.0'."
	@echo "Nullify version in docs/antora.yml."
	@echo "Commit the changes."
	@echo "Push commits and tag."

## up-cache: Start Redis.
.PHONY: up-cache
up-cache:
	@docker run \
	--rm \
	-d \
	--name python-sdk-redis \
	-p 6379:6379 \
	redis:6.0.9  # apps use 6.0.9 or 6.2.3

# down-cache: Stop Redis.
.PHONY: down-cache
down-cache:
	@docker stop python-sdk-redis
