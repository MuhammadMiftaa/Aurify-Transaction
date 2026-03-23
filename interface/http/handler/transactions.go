package handler

import (
	"net/http"
	"strings"

	"refina-transaction/config/log"
	"refina-transaction/internal/service"
	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/utils/data"

	"github.com/gin-gonic/gin"
)

const errInvalidRequestBody = "invalid request body"

type TransactionHandler struct {
	transactionServ service.TransactionsService
}

func NewTransactionHandler(transactionServ service.TransactionsService) *TransactionHandler {
	return &TransactionHandler{transactionServ}
}

func (transactionHandler *TransactionHandler) GetAllTransactions(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	transactions, err := transactionHandler.transactionServ.GetAllTransactions(ctx)
	if err != nil {
		log.Error(data.LogGetAllTransactionsFailed, map[string]any{
			"service":    data.TransactionService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get all transactions data",
		"data":       transactions,
	})
}

func (transactionHandler *TransactionHandler) GetTransactionByID(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	transaction, err := transactionHandler.transactionServ.GetTransactionByID(ctx, id)
	if err != nil {
		log.Error(data.LogGetTransactionByIDFailed, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": id,
			"error":          err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get transaction data by ID",
		"data":       transaction,
	})
}

func (transactionHandler *TransactionHandler) GetTransactionsByUserID(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	var ids []string
	if err := c.BindJSON(&ids); err != nil {
		log.Warn(data.LogGetTransactionsByUserIDBadRequest, map[string]any{
			"service":    data.TransactionService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		c.JSON(http.StatusBadRequest, gin.H{
			"statusCode": 400,
			"status":     false,
			"message":    errInvalidRequestBody,
		})
		return
	}

	transactions, err := transactionHandler.transactionServ.GetTransactionsByWalletIDs(ctx, ids)
	if err != nil {
		log.Error(data.LogGetTransactionsByWalletIDsFailed, map[string]any{
			"service":    data.TransactionService,
			"request_id": requestID,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Get transactions data by wallet ID",
		"data":       transactions,
	})
}

func (transactionHandler *TransactionHandler) CreateTransaction(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	types := c.Param("type")

	var (
		transactionCreated any
		err                error
	)

	if types != "fund_transfer" {
		var transaction dto.TransactionsRequest
		if err := c.ShouldBindJSON(&transaction); err != nil {
			log.Warn(data.LogCreateTransactionBadRequest, map[string]any{
				"service":    data.TransactionService,
				"request_id": requestID,
				"type":       types,
				"error":      err.Error(),
			})
			c.JSON(http.StatusBadRequest, gin.H{
				"statusCode": 400,
				"status":     false,
				"message":    errInvalidRequestBody,
			})
			return
		}
		transactionCreated, err = transactionHandler.transactionServ.CreateTransaction(ctx, transaction)
	} else {
		var transaction dto.FundTransferRequest
		if err := c.ShouldBindJSON(&transaction); err != nil {
			log.Warn(data.LogCreateFundTransferBadRequest, map[string]any{
				"service":    data.TransactionService,
				"request_id": requestID,
				"error":      err.Error(),
			})
			c.JSON(http.StatusBadRequest, gin.H{
				"statusCode": 400,
				"status":     false,
				"message":    errInvalidRequestBody,
			})
			return
		}
		transactionCreated, err = transactionHandler.transactionServ.FundTransfer(ctx, transaction)
	}

	if err != nil {
		log.Error(data.LogCreateTransactionServiceFailed, map[string]any{
			"service":    data.TransactionService,
			"request_id": requestID,
			"type":       types,
			"error":      err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	log.Info(data.LogTransactionCreatedHTTP, map[string]any{
		"service":    data.TransactionService,
		"request_id": requestID,
		"type":       types,
	})

	c.JSON(http.StatusCreated, gin.H{
		"statusCode": 201,
		"status":     true,
		"message":    "Create transaction data",
		"data":       transactionCreated,
	})
}

func (transactionHandler *TransactionHandler) UploadAttachment(c *gin.Context) {
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	ID := c.Param("id")
	var payload dto.Attachments
	if err := c.Bind(&payload); err != nil {
		log.Warn(data.LogUploadAttachmentBadRequest, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": ID,
			"error":          err.Error(),
		})
		c.JSON(http.StatusBadRequest, gin.H{
			"statusCode": 400,
			"status":     false,
			"message":    errInvalidRequestBody,
		})
		return
	}

	ctx := c.Request.Context()

	attachment, err := transactionHandler.transactionServ.UploadAttachment(ctx, nil, ID, payload.Files)
	if err != nil {
		log.Error(data.LogUploadAttachmentFailed, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": ID,
			"error":          err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Upload attachment success",
		"data":       attachment,
	})
}

func (transactionHandler *TransactionHandler) UpdateTransaction(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	var transaction dto.TransactionsRequest
	if err := c.ShouldBindJSON(&transaction); err != nil {
		log.Warn(data.LogUpdateTransactionBadRequest, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": id,
			"error":          err.Error(),
		})
		c.JSON(http.StatusBadRequest, gin.H{
			"statusCode": 400,
			"status":     false,
			"message":    errInvalidRequestBody,
		})
		return
	}

	transactionUpdated, err := transactionHandler.transactionServ.UpdateTransaction(ctx, id, transaction)
	if err != nil {
		log.Error(data.LogUpdateTransactionFailed, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": id,
			"error":          err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Update transaction data",
		"data":       transactionUpdated,
	})
}

func (transactionHandler *TransactionHandler) DeleteTransaction(c *gin.Context) {
	ctx := c.Request.Context()
	requestID, _ := c.Get(data.REQUEST_ID_LOCAL_KEY)

	id := c.Param("id")

	transactionDeleted, err := transactionHandler.transactionServ.DeleteTransaction(ctx, id)
	if err != nil {
		log.Error(data.LogDeleteTransactionHTTPFailed, map[string]any{
			"service":        data.TransactionService,
			"request_id":     requestID,
			"transaction_id": id,
			"error":          err.Error(),
		})
		statusCode, message := mapServiceError(err)
		c.JSON(statusCode, gin.H{
			"statusCode": statusCode,
			"status":     false,
			"message":    message,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"statusCode": 200,
		"status":     true,
		"message":    "Delete transaction data",
		"data":       transactionDeleted,
	})
}

// mapServiceError translates service errors to HTTP status codes and safe client messages.
func mapServiceError(err error) (int, string) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, "resource not found"
	case strings.Contains(msg, "invalid"),
		strings.Contains(msg, "insufficient"):
		return http.StatusBadRequest, "invalid request"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}