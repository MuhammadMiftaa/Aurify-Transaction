package data

// Service field logging constants
const (
	MainService               = "main"
	EnvService                = "env"
	DatabaseService           = "database"
	MinioService              = "minio"
	RabbitmqService           = "rabbitmq"
	GRPCClientService         = "grpc_client"
	GRPCServerService         = "grpc_server"
	HTTPServerService         = "http_server"
	OutboxService             = "outbox"
	TransactionService        = "transaction"
	CategoryService           = "category"
	InvestmentConsumerService = "investment_consumer"
)

// Message field logging constants
const (
	// --- startup / env ---
	LogEnvVarMissing = "env_var_missing"

	// --- infrastructure setup ---
	LogDBSetupSuccess       = "db_setup_success"
	LogMinioSetupSuccess    = "minio_setup_success"
	LogRabbitmqSetupSuccess = "rabbitmq_setup_success"

	// --- outbox publisher ---
	LogOutboxPublisherStarted          = "outbox_publisher_started"
	LogOutboxPublishPendingFailed      = "outbox_publish_pending_failed"
	LogOutboxMessagePublishFailed      = "outbox_message_publish_failed"
	LogOutboxMessageMaxRetriesExceeded = "outbox_message_max_retries_exceeded"
	LogOutboxIncrementRetriesFailed    = "outbox_increment_retries_failed"
	LogOutboxMarkPublishedFailed       = "outbox_mark_published_failed"
	LogOutboxMessagePublished          = "outbox_message_published"
	LogOutboxCleanupFailed             = "outbox_cleanup_failed"

	// --- gRPC client ---
	LogGRPCClientSetupFailed  = "grpc_client_setup_failed"
	LogGRPCClientSetupSuccess = "grpc_client_setup_success"
	LogGRPCClientCloseFailed  = "grpc_client_close_failed"

	// --- gRPC server ---
	LogGRPCServerSetupFailed = "grpc_server_setup_failed"
	LogGRPCServerStarted     = "grpc_server_started"
	LogGRPCServerServeFailed = "grpc_server_serve_failed"

	// --- HTTP server ---
	LogHTTPServerStarted     = "http_server_started"
	LogHTTPServerStartFailed = "http_server_start_failed"

	// --- shutdown ---
	LogShutdownSignalReceived      = "shutdown_signal_received"
	LogHTTPServerShutdownFailed    = "http_server_shutdown_failed"
	LogGRPCClientShutdownFailed    = "grpc_client_shutdown_failed"
	LogRabbitmqCloseFailed         = "rabbitmq_close_failed"
	LogDBCloseFailed               = "db_close_failed"
	LogShutdownCompletedWithErrors = "shutdown_completed_with_errors"
	LogShutdownCompleted           = "shutdown_completed"

	// --- gRPC server handlers (transaction) ---
	LogGetTransactionsFailed         = "get_transactions_failed"
	LogGetUserTransactionsFailed     = "get_user_transactions_failed"
	LogGetUserTransactionsSuccess    = "get_user_transactions_success"
	LogGetTransactionByIDGRPCFailed  = "grpc_get_transaction_by_id_failed"
	LogGetTransactionByIDGRPCSuccess = "grpc_get_transaction_by_id_success"
	LogCreateTransactionFailed       = "create_transaction_failed"
	LogTransactionCreated            = "transaction_created"
	LogCreateFundTransferFailed      = "create_fund_transfer_failed"
	LogFundTransferCreated           = "fund_transfer_created"
	LogUpdateTransactionGRPCFailed   = "grpc_update_transaction_failed"
	LogTransactionUpdated            = "transaction_updated"
	LogDeleteTransactionFailed       = "delete_transaction_failed"
	LogTransactionDeleted            = "transaction_deleted"
	LogStreamSendFailed              = "stream_send_failed"

	// --- gRPC server handlers (category) ---
	LogGetCategoriesGRPCFailed  = "grpc_get_categories_failed"
	LogGetCategoriesGRPCSuccess = "grpc_get_categories_success"

	// --- gRPC server handlers (attachment) ---
	LogGetAttachmentsByTxnIDFailed  = "grpc_get_attachments_by_txn_id_failed"
	LogGetAttachmentsByTxnIDSuccess = "grpc_get_attachments_by_txn_id_success"
	LogCreateAttachmentGRPCFailed   = "grpc_create_attachment_failed"
	LogAttachmentCreated            = "grpc_attachment_created"
	LogDeleteAttachmentGRPCFailed   = "grpc_delete_attachment_failed"
	LogAttachmentDeleted            = "grpc_attachment_deleted"

	// --- http handler (transaction) ---
	LogGetAllTransactionsFailed          = "get_all_transactions_failed"
	LogGetTransactionByIDFailed          = "get_transaction_by_id_failed"
	LogGetTransactionsByUserIDBadRequest = "get_transactions_by_user_id_bad_request"
	LogGetTransactionsByWalletIDsFailed  = "get_transactions_by_wallet_ids_failed"
	LogCreateTransactionBadRequest       = "create_transaction_bad_request"
	LogCreateFundTransferBadRequest      = "create_fund_transfer_bad_request"
	LogCreateTransactionServiceFailed    = "create_transaction_service_failed"
	LogTransactionCreatedHTTP            = "transaction_created"
	LogUploadAttachmentBadRequest        = "upload_attachment_bad_request"
	LogUploadAttachmentFailed            = "upload_attachment_failed"
	LogUpdateTransactionBadRequest       = "update_transaction_bad_request"
	LogUpdateTransactionFailed           = "update_transaction_failed"
	LogDeleteTransactionHTTPFailed       = "delete_transaction_failed"

	// --- http handler (category) ---
	LogGetAllCategoriesFailed    = "get_all_categories_failed"
	LogGetCategoryByIDFailed     = "get_category_by_id_failed"
	LogGetCategoriesByTypeFailed = "get_categories_by_type_failed"
	LogCreateCategoryBadRequest  = "create_category_bad_request"
	LogCreateCategoryFailed      = "create_category_failed"
	LogCategoryCreated           = "category_created"
	LogUpdateCategoryBadRequest  = "update_category_bad_request"
	LogUpdateCategoryFailed      = "update_category_failed"
	LogDeleteCategoryFailed      = "delete_category_failed"

	// --- investment event consumer ---
	LogInvestmentConsumerStarted        = "investment_consumer_started"
	LogInvestmentConsumerStopped        = "investment_consumer_stopped"
	LogInvestmentConsumerFailed         = "investment_consumer_failed"
	LogInvestmentEventHandleFailed      = "investment_event_handle_failed"
	LogInvestmentEventUnknown           = "investment_event_unknown"
	LogInvestmentEventSkipped           = "investment_event_skipped"
	LogInvestmentBuyTransactionCreated  = "investment_buy_transaction_created"
	LogInvestmentSellTransactionCreated = "investment_sell_transaction_created"

	// --- admin consumer ---
	AdminConsumerService      = "admin_consumer"
	LogAdminConsumerStarted   = "admin_consumer_started"
	LogAdminConsumerStopped   = "admin_consumer_stopped"
	LogAdminConsumerFailed    = "admin_consumer_failed"
	LogAdminEventHandleFailed = "admin_event_handle_failed"
	LogAdminEventUnknown      = "admin_event_unknown_action"
	LogAdminCategoryCreated   = "admin_category_created"
	LogAdminCategoryUpdated   = "admin_category_updated"
	LogAdminCategoryDeleted   = "admin_category_deleted"

	// --- gRPC server (admin master data) ---
	LogListCategoriesSuccess    = "list_categories_success"
	LogListCategoriesFailed     = "list_categories_failed"
	LogGetCategoryDetailSuccess = "get_category_detail_success"
	LogGetCategoryDetailFailed  = "get_category_detail_failed"
)
