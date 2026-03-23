package routes

import (
	"refina-transaction/config/miniofs"
	"refina-transaction/interface/grpc/client"
	"refina-transaction/interface/http/handler"
	"refina-transaction/internal/repository"
	"refina-transaction/internal/service"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TransactionRoutes(version *gin.Engine, db *gorm.DB, minio *miniofs.MinIOManager) {
	txManager := repository.NewTxManager(db)
	transactionRepo := repository.NewTransactionRepository(db)
	walletRepo := client.NewWalletClient(client.GetManager().GetWalletClient())
	categoryRepo := repository.NewCategoryRepository(db)
	attachmentRepo := repository.NewAttachmentsRepository(db)
	outboxRepository := repository.NewOutboxRepository(db)

	transactionServ := service.NewTransactionService(txManager, transactionRepo, walletRepo, categoryRepo, attachmentRepo, outboxRepository, minio)
	transactionHandler := handler.NewTransactionHandler(transactionServ)

	transaction := version.Group("/transactions")

	transaction.GET("", transactionHandler.GetAllTransactions)
	transaction.GET(":id", transactionHandler.GetTransactionByID)
	transaction.GET("user", transactionHandler.GetTransactionsByUserID)
	transaction.POST(":type", transactionHandler.CreateTransaction)
	transaction.POST("attachment/:id", transactionHandler.UploadAttachment)
	transaction.PUT(":id", transactionHandler.UpdateTransaction)
	transaction.DELETE(":id", transactionHandler.DeleteTransaction)
}