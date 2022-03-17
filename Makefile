.DEFAULT_GOAL := all
black = black --target-version py39 blazer
isort = isort --profile black blazer

.PHONY: depends
depends:
	bash ./bin/depends.sh

.PHONY: init
init: depends
	echo "Setting up virtual environment in venv/"
	python3 -m venv venv
	echo "Virtual environment complete."

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	mypy --show-error-codes  blazer
	flake8 --ignore=E203,F841,E501,E722,W503  blazer
	$(isort) --check-only --df
	$(black) --check --diff

.PHONY: install
install: depends init
	pip install -r requirements.txt
	python setup.py install
	python setup.py clean

.PHONY: update
update: format lint
	pip freeze | grep -v blazer > requirements.txt
	git add docs bin blazer requirements.txt Makefile
	git commit --allow-empty -m "Updates"
	git push origin main
	python setup.py install

.PHONY: docs
docs:
	cd docs
	make -C docs html

.PHONY: release
release: update test docs 
	bash ./bin/tag.sh

.PHONY: clean
clean:
	python setup.py clean

.PHONY: tests
tests: format lint
	python setup.py install
	bash ./bin/tests.sh

.PHONY: all
all: format lint update docs install tests clean