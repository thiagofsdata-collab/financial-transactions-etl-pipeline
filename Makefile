# ================================================================
# Financial Transactions ETL Pipeline — Makefile
# Commands for Windows PowerShell users: use 'make <command>'
# Requires 'make' installed via: choco install make
# Alternative: run the commands directly in PowerShell
# ================================================================

.PHONY: help setup up down run test clean

help:
	@echo "Available commands:"
	@echo " make setup - Create .env from .env.example"
	@echo " make up - Start PostgreSQL container"
	@echo " make down - Stop PostgreSQL container"
	@echo " make run - Run the ETL pipeline"
	@echo " make test - Run pytest test suite"
	@echo " make clean - Stop container and remove volumes"

setup:
	cp .env.example .env
	pip install -r requirements.txt

up:
	docker-compose up -d
	docker ps

down:
	docker-compose down

run:
	python -m src.pipeline

test:
	pytest tests/ -v

clean:
	docker-compose down -v