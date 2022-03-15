.DEFAULT_GOAL := all
black = black --target-version py39 blazer
isort = isort --profile black blazer

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	flake8 --ignore=E203,F841,E501,E722,W503  blazer
	$(isort) --check-only --df
	$(black) --check --diff

.PHONY: install
install:
	python setup.py install
	python setup.py clean

.PHONY: update
update:
	git add blazer
	git commit -m "Updates"
	git push origin main
	python setup.py install

.PHONY: release
release:
	bash ./bin/tag.sh

.PHONY: clean
clean:
	python setup.py clean

.PHONY: tests
tests:
	bash ./bin/tests.sh

.PHONY: all
all: format lint install test