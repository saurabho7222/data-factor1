.PHONY: install test coverage lint typecheck quality package-check docker-build docker-test compose-config compose-smoke security

install:
	python3 -m pip install -e .
	python3 -m pip install -r requirements-dev.txt

test:
	python3 -m pytest tests -v

coverage:
	python3 -m pytest --cov=eventforge --cov-report=term-missing --cov-report=xml --cov-fail-under=90

lint:
	python3 -m ruff check .

typecheck:
	python3 -m mypy src/eventforge scripts

quality: lint typecheck coverage

package-check:
	rm -rf dist .package-check-venv
	python3 -m pip wheel . --no-deps --wheel-dir dist
	python3 -m venv .package-check-venv
	.package-check-venv/bin/python -m pip install dist/*.whl
	.package-check-venv/bin/eventforge --help
	.package-check-venv/bin/python -c "from eventforge.api import create_app; assert callable(create_app)"
	rm -rf .package-check-venv

docker-build:
	docker build -t eventforge .

docker-test: docker-build
	docker run --rm eventforge python3 -m pytest tests -v

compose-config:
	docker compose config --quiet

compose-smoke:
	@set -eu; \
		mkdir -p .local; \
		rm -f .local/eventforge.db .local/eventforge.db-shm .local/eventforge.db-wal; \
		trap 'docker compose down --remove-orphans >/dev/null 2>&1 || true' EXIT; \
		docker compose up --build -d api worker; \
		python3 scripts/compose_smoke.py

security:
	python3 -m pip_audit -r requirements.txt
	python3 -m pip_audit -r requirements-dev.txt
