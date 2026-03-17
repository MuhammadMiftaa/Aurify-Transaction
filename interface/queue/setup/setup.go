package setup

import (
	"context"

	"refina-transaction/config/db"
	"refina-transaction/config/miniofs"
	grpcclient "refina-transaction/interface/grpc/client"
	"refina-transaction/interface/queue/client"
	"refina-transaction/interface/queue/consumer"
	"refina-transaction/internal/repository"
	"refina-transaction/internal/service"
)

func SetupQueueConsumers(ctx context.Context, dbInstance db.DatabaseClient, minioInstance *miniofs.MinIOManager, rmq client.RabbitMQClient) {
	txManager := repository.NewTxManager(dbInstance.GetDB())
	transactionRepo := repository.NewTransactionRepository(dbInstance.GetDB())
	walletClient := grpcclient.NewWalletClient(grpcclient.GetManager().GetWalletClient())
	categoryRepo := repository.NewCategoryRepository(dbInstance.GetDB())
	attachmentRepo := repository.NewAttachmentsRepository(dbInstance.GetDB())
	outboxRepo := repository.NewOutboxRepository(dbInstance.GetDB())

	transactionService := service.NewTransactionService(
		txManager,
		transactionRepo,
		walletClient,
		categoryRepo,
		attachmentRepo,
		outboxRepo,
		minioInstance,
	)

	investmentConsumer := consumer.NewInvestmentEventConsumer(rmq, transactionService)
	go investmentConsumer.Start(ctx)

	// Admin event consumer for master data (transaction categories)
	categoryService := service.NewCategoriesService(txManager, categoryRepo)
	adminConsumer := consumer.NewAdminEventConsumer(rmq, categoryService)
	go adminConsumer.Start(ctx)
}
