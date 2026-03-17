package data

import "time"

var (
	DEVELOPMENT_MODE = "development"
	STAGING_MODE     = "staging"
	PRODUCTION_MODE  = "production"

	OUTBOX_PUBLISH_EXCHANGE          = "refina_microservice"
	OUTBOX_PUBLISH_INTERVAL          = 5 * time.Second
	OUTBOX_PUBLISH_BATCH             = 100
	OUTBOX_PUBLISH_MAX_RETRIES       = 5
	OUTBOX_EVENT_TRANSACTION_CREATED = "transaction.created"
	OUTBOX_EVENT_TRANSACTION_UPDATED = "transaction.updated"
	OUTBOX_EVENT_TRANSACTION_DELETED = "transaction.deleted"

	EVENT_INVESTMENT_QUEUE = "refina-investments"
	// Investment event routing keys (consumed from investment-service)
	EVENT_INVESTMENT_BUY  = "investment.buy"
	EVENT_INVESTMENT_SELL = "investment.sell"

	// Admin exchange and routing keys
	ADMIN_EXCHANGE    = "refina_admin"
	ADMIN_QUEUE       = "refina-admin-transaction-categories"
	ADMIN_ROUTING_KEY = "master.transaction_categories"

	CATEGORY_ID_INITIAL_DEPOSIT        = "00000000-0000-0000-0000-000000000000"
	CATEGORY_ID_FUND_TRANSFER          = "00000000-0000-0000-0000-000000000010"
	CATEGORY_ID_FUND_TRANSFER_CASH_IN  = "00000000-0000-0000-0000-000000000011"
	CATEGORY_ID_FUND_TRANSFER_CASH_OUT = "00000000-0000-0000-0000-000000000012"
	CATEGORY_ID_INVESTMENT_BUY         = "66239d17-3320-4c98-9b8c-fb8d84827085"
	CATEGORY_ID_INVESTMENT_SELL        = "635fdfd1-31f4-472c-8d52-e59a66c31351"

	// REQUEST_ID_HEADER is the standard header name used to propagate request IDs.
	REQUEST_ID_HEADER = "X-Request-ID"
	// REQUEST_ID_LOCAL_KEY is the key used to store the request ID in Gin's context locals.
	REQUEST_ID_LOCAL_KEY = "request_id"
)
