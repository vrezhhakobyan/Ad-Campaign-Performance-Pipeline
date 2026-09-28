DATE ?= 2026-09-01

install:
	pip install -r requirements.txt

test:
	python -m pytest tests -q

data:
	python -m data_generator.generate_data --date $(DATE) --rows 50000

run-local: data
	python -m jobs.clean_events --date $(DATE)
	python -m jobs.quality_check --date $(DATE)
	python -m jobs.build_metrics --date $(DATE)

airflow:
	docker compose up --build
