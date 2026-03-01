PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
UVICORN := $(VENV)/bin/uvicorn

.PHONY: venv install dev run test compile migrate migrate-surgical

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt

dev: install
	$(PIP) install -r requirements.txt

run:
	$(UVICORN) main:app --host 0.0.0.0 --port 8000

test:
	$(PY) -m unittest discover -s tests -p 'test_*.py'

compile:
	$(PY) -m compileall app main.py

migrate:
	$(VENV)/bin/alembic upgrade head

migrate-surgical:
	$(VENV)/bin/alembic -c alembic_surgical.ini upgrade head
