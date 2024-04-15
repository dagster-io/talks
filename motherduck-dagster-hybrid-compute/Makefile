.PHONY: help

help:
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'


clean:  ## Remove unnecessary artifacts
	rm -rf ./data/db/*
	rm -rf ./data/raw/*

install:  ## Install python dependencies
	pip install -e '.[dev]'

dev:  ## Run Dagster development environment
	dagster dev

evidence-install:  ## Install Evidence dependencies
	npm --prefix ./dbt_project/reports install

evidence-source:  ## Pull Evidence source data
	npm --prefix ./dbt_project/reports run source

evidence-build:  ## Build Evidence reports
	npm --prefix ./dbt_project/reports run build

evidence-preview:  ## Preview built evidence reports on port 4000
	npm --prefix ./dbt_project/reports run preview -- --port 4000

evidence:  ## Run Evidence development environment on port 4000
	npm --prefix ./dbt_project/reports run dev -- --port 4000

ruff:  ## Format Python code with `ruff`
	ruff check --select I --fix .
	ruff format .
