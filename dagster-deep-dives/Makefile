install:
	pip install -e .[dev]
	cp .env.example .env

test:
	pytest

run: clean
	@bash -c ' \
	echo "Select the example project to run:"; \
	select option in "automation" "resources-and-configuration" "partitions"; do \
	    case $$option in \
	        "automation") \
	            dagster dev -m dagster_deep_dives.automation; \
	            break;; \
	        "resources-and-configuration") \
	            dagster dev -m dagster_deep_dives.resources_and_configurations; \
	            break;; \
	        "partitions") \
	            dagster dev -m dagster_deep_dives.partitions; \
	            break;; \
	        *) \
	            echo "Invalid option"; \
	            ;; \
	    esac \
	done'

clean:
	@echo "Cleaning up files..."
	rm data.duckdb local.duckdb staging.duckdb || true
	rm -f dagster_deep_dives/data/bird_data/FeederWatch*.zip
	rm -f dagster_deep_dives/data/bird_data/PFW_all*.csv
	rm -rf dagster_deep_dives/data/bird_data/__MACOSX
	rm -rf dagster_deep_dives/data/daily_order_aggregates
	rm -rf dagster_deep_dives/data/dim_customers
	rm -rf dagster_deep_dives/data/dim_destinations
	rm -rf dagster_deep_dives/data/dim_items
	rm -rf dagster_deep_dives/data/dim_promotions
	rm -rf dagster_deep_dives/data/fct_orders
	rm -rf dagster_deep_dives/data/monthly_order_aggregates
	rm -rf dagster_deep_dives/data/weekly_order_aggregates
	@echo
