#!/usr/bin/env bash
set -euo pipefail

curl --fail-with-body --silent --show-error \
  -X POST http://localhost:3000/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-request-1' \
  -d '{"customerId":"customer-1","items":[{"productId":"product-1","quantity":2}],"demo":{"failAnalytics":true,"inventoryDelayMs":8000}}'
echo
