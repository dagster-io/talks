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
	cd dbt_project && npm --prefix ./reports install
	cd ..

evidence-run:
	cd dbt_project && npm --prefix ./reports run dev -- --port 4000
