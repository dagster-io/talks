help:
	echo "Help!"

clean:
	rm -rf ./data/db/*
	rm -rf ./data/raw/*

install:
	pip install -e '.[dev]'

dev:
	dagster dev

evidence-install:
	npm --prefix ./dbt_project/reports install

evidence-source:
	npm --prefix ./dbt_project/reports run source

evidence:
	npm --prefix ./dbt_project/reports run dev -- --port 4000

ruff:
	ruff check --select I --fix .
	ruff format .
