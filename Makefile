SHELL := /bin/bash

API_URL ?= http://localhost:3000
CUSTOMER_ID ?= customer-cli
PRODUCT_ID ?= product-1
QUANTITY ?= 1
FAIL_ANALYTICS ?= false
INVENTORY_DELAY_MS ?= 0

.PHONY: help up down logs request request-normal request-failure request-slow request-demo activity events dlt test

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build and start Kafka and all services
	docker compose up --build -d

down: ## Stop services and remove Kafka data
	docker compose down -v

logs: ## Follow all service logs
	docker compose logs -f

request: ## Create an order using the variables at the top of this file
	@curl --fail-with-body --silent --show-error \
		-X POST "$(API_URL)/orders" \
		-H 'content-type: application/json' \
		-d '{"customerId":"$(CUSTOMER_ID)","items":[{"productId":"$(PRODUCT_ID)","quantity":$(QUANTITY)}],"demo":{"failAnalytics":$(FAIL_ANALYTICS),"inventoryDelayMs":$(INVENTORY_DELAY_MS)}}'
	@echo

request-normal: ## Create an order with no simulated delay or failure
	@$(MAKE) --no-print-directory request FAIL_ANALYTICS=false INVENTORY_DELAY_MS=0

request-failure: ## Create an order whose analytics consumer reaches the DLT
	@$(MAKE) --no-print-directory request FAIL_ANALYTICS=true INVENTORY_DELAY_MS=0

request-slow: ## Create an order whose inventory consumer takes 8 seconds
	@$(MAKE) --no-print-directory request FAIL_ANALYTICS=false INVENTORY_DELAY_MS=8000

request-demo: ## Create an order with both slow inventory and analytics failure
	@$(MAKE) --no-print-directory request FAIL_ANALYTICS=true INVENTORY_DELAY_MS=8000

activity: ## Print the live consumer activity JSON
	@curl --fail-with-body --silent --show-error "$(API_URL)/activity"
	@echo

events: ## Print messages shown in the order.created topic view
	@curl --fail-with-body --silent --show-error "$(API_URL)/events"
	@echo

dlt: ## Print messages shown in the dead-letter topic view
	@curl --fail-with-body --silent --show-error "$(API_URL)/dead-letters"
	@echo

test: ## Run unit tests
	npm test
