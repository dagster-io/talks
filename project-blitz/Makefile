install:
	pip install -e .[dev]
	cp .env.example .env

test:
	pytest

run:
	rm data.duckdb || true
	env -u DAGSTER_HOME dagster dev

clean:
	rm -f project_blitz/data/bird_data/FeederWatch*.zip
	rm -f project_blitz/data/bird_data/PFW_all*.csv
	rm -rf project_blitz/data/bird_data/__MACOSX
