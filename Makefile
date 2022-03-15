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
	
.PHONY: all
all: format lint install