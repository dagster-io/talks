help:
	echo "Help!"

clean:
	rm -rf ./data/db/*
	rm -rf ./data/raw/*

install:
	pip install -e .'[dev]'

reports-install:
	cd dbt_project && npm --prefix ./reports install
	cd ..

dev:
	dagster dev

run-evidence:
	cd dbt_project && npm --prefix ./reports run dev -- --port 4000
