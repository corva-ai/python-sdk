docs_dir = docs
docs_build_dir = build
srcs = src $(docs_dir)/src tests setup.py

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
install-lint:
	@pip install -U -r requirements-lint.txt

## test: Run tests and show code coverage.
.PHONY: test
test:
	@pytest --cov

## testcov: Show HTML code coverage.
.PHONY: testcov
testcov: test
	@coverage html
	@x-www-browser htmlcov/index.html

## lint: Run linter.
.PHONY: lint
lint:
	@flake8 $(srcs)

## format: Format all files.
.PHONY: format
format:
	@isort --force-single-line-imports --quiet $(srcs)
	@autoflake --remove-all-unused-imports --recursive --remove-unused-variables \
		--in-place $(srcs)
	@black --skip-string-normalization --quiet $(srcs)
	@isort --quiet $(srcs)

## docs: Generate docs.
.PHONY: docs
docs:
	@docker run --rm -v $(CURDIR)/$(docs_dir)/:/documents/ \
			asciidoctor/docker-asciidoctor:1.7 asciidoctor -D $(docs_build_dir) '*.adoc'

## docs-view: Show HTML docs.
.PHONY: docs-view
docs-view: docs
	@x-www-browser $(docs_dir)/$(docs_build_dir)/index.html

## clean: Clean autogenerated files.
.PHONY: clean
clean:
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('*.py[co]'): p.unlink()"
	@-python3 -Bc "for p in __import__('pathlib').Path('.').rglob('__pycache__'): p.rmdir()"
	@-rm -rf src/*.egg-info
	@-rm -rf .pytest_cache
	@-rm -rf htmlcov
	@-rm .coverage
	@-sudo rm -rf $(docs_dir)/$(docs_build_dir)

## release: How to release a new version.
.PHONY: release
release:
	@echo "Checkout the master branch."
	@echo "Update src/version.py with new version."
	@echo "Update $(docs_dir)/index.adoc with new version."
	@echo "Update CHANGELOG.md."
	@echo "Commit the changes."
	@echo "Create tag like "v1.0.0"."
	@echo "Push commit and tag."
