install:
	pip install -e .[dev]
	cp .env.example .env

test:
	pytest

run: clean
	@bash -c ' \
	echo "Select the example project to run:"; \
	select option in "automation" "resources-and-configuration"; do \
	    case $$option in \
	        "automation") \
	            dagster dev -m project_blitz.automation; \
	            break;; \
	        "resources-and-configuration") \
	            dagster dev -m project_blitz.resources_and_configurations; \
	            break;; \
	        *) \
	            echo "Invalid option"; \
	            ;; \
	    esac \
	done'

clean:
	@echo "Cleaning up files..."
	rm data.duckdb local.duckdb staging.duckdb || true
	rm -f project_blitz/data/bird_data/FeederWatch*.zip
	rm -f project_blitz/data/bird_data/PFW_all*.csv
	rm -rf project_blitz/data/bird_data/__MACOSX
	@echo
