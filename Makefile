.DEFAULT_GOAL := all
black = black --target-version py39 blazer
isort = isort blazer

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	flake8 --ignore=F841,E501,E722,W503  blazer
	$(isort) --check-only --df
	$(black) --check --diff
