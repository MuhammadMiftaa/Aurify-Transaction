package service

import (
	"context"
	"encoding/json"
	"fmt"

	"refina-transaction/config/miniofs"
	"refina-transaction/interface/grpc/client"
	"refina-transaction/internal/repository"
	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/types/model"
	"refina-transaction/internal/utils"
	helper "refina-transaction/internal/utils"
	"refina-transaction/internal/utils/data"

	"github.com/google/uuid"
)

// Error message constants to avoid duplication (go:S1192)
const (
	errTransactionNotFound  = "transaction not found [id=%s]: %w"
	errWalletNotFound       = "wallet not found [id=%s]: %w"
	errInvalidTransactionType = "invalid transaction type [type=%s]"
)

type TransactionsService interface {
	GetAllTransactions(ctx context.Context) ([]dto.TransactionsResponse, error)
	GetTransactionByID(ctx context.Context, id string) (dto.TransactionsResponse, error)
	GetTransactionsByWalletIDs(ctx context.Context, ids []string) ([]dto.TransactionsResponse, error)
	GetTransactionsByCursor(ctx context.Context, q repository.CursorQuery) ([]dto.TransactionsResponse, int64, error)
	CreateTransaction(ctx context.Context, transaction dto.TransactionsRequest) (dto.TransactionsResponse, error)
	FundTransfer(ctx context.Context, transaction dto.FundTransferRequest) (dto.FundTransferResponse, error)
	UploadAttachment(ctx context.Context, tx repository.Transaction, transactionID string, files []string) ([]dto.AttachmentsResponse, error)
	UpdateTransaction(ctx context.Context, id string, transaction dto.TransactionsRequest) (dto.TransactionsResponse, error)
	DeleteTransaction(ctx context.Context, id string) (dto.TransactionsResponse, error)
}

type transactionsService struct {
	txManager        repository.TxManager
	transactionRepo  repository.TransactionsRepository
	categoryRepo     repository.CategoriesRepository
	attachmentRepo   repository.AttachmentsRepository
	outboxRepository repository.OutboxRepository
	minio            *miniofs.MinIOManager
	walletClient     client.WalletClient
}

func NewTransactionService(txManager repository.TxManager, transactionRepo repository.TransactionsRepository, walletRepo client.WalletClient, categoryRepo repository.CategoriesRepository, attachmentRepo repository.AttachmentsRepository, outboxRepository repository.OutboxRepository, minio *miniofs.MinIOManager) TransactionsService {
	return &transactionsService{
		txManager:        txManager,
		transactionRepo:  transactionRepo,
		categoryRepo:     categoryRepo,
		attachmentRepo:   attachmentRepo,
		outboxRepository: outboxRepository,
		minio:            minio,
		walletClient:     walletRepo,
	}
}

func (transaction_serv *transactionsService) GetAllTransactions(ctx context.Context) ([]dto.TransactionsResponse, error) {
	transactions, err := transaction_serv.transactionRepo.GetAllTransactions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("get all transactions: %w", err)
	}

	transactionResponses := make([]dto.TransactionsResponse, 0, len(transactions))
	for _, transaction := range transactions {
		transactionResponse := helper.ConvertToResponseType(transaction).(dto.TransactionsResponse)
		transactionResponses = append(transactionResponses, transactionResponse)
	}

	return transactionResponses, nil
}

func (transaction_serv *transactionsService) GetTransactionByID(ctx context.Context, id string) (dto.TransactionsResponse, error) {
	transaction, err := transaction_serv.transactionRepo.GetTransactionByID(ctx, nil, id)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf(errTransactionNotFound, id, err)
	}

	transactionResponse := helper.ConvertToResponseType(transaction).(dto.TransactionsResponse)

	attachments, err := transaction_serv.attachmentRepo.GetAttachmentsByTransactionID(ctx, nil, transaction.ID.String())
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("get attachments [transaction_id=%s]: %w", id, err)
	}

	if len(attachments) > 0 {
		for _, attachment := range attachments {
			if attachment.Image != "" {
				result := dto.AttachmentsResponse{
					ID:            attachment.ID.String(),
					TransactionID: attachment.TransactionID.String(),
					Image:         attachment.Image,
					Format:        attachment.Format,
					Size:          attachment.Size,
				}
				transactionResponse.Attachments = append(transactionResponse.Attachments, result)
			}
		}
	} else {
		transactionResponse.Attachments = make([]dto.AttachmentsResponse, 0, len(attachments))
	}

	return transactionResponse, nil
}

func (transaction_serv *transactionsService) GetTransactionsByWalletIDs(ctx context.Context, ids []string) ([]dto.TransactionsResponse, error) {
	transactions, err := transaction_serv.transactionRepo.GetTransactionsByWalletIDs(ctx, nil, ids)
	if err != nil {
		return nil, fmt.Errorf("get transactions by wallet ids: %w", err)
	}

	transactionResponses := make([]dto.TransactionsResponse, 0, len(transactions))
	for _, transaction := range transactions {
		transactionResponse := helper.ConvertToResponseType(transaction).(dto.TransactionsResponse)
		transactionResponses = append(transactionResponses, transactionResponse)
	}

	return transactionResponses, nil
}

func (transaction_serv *transactionsService) GetTransactionsByCursor(ctx context.Context, q repository.CursorQuery) ([]dto.TransactionsResponse, int64, error) {
	transactions, total, err := transaction_serv.transactionRepo.GetTransactionsByCursor(ctx, nil, q)
	if err != nil {
		return nil, 0, fmt.Errorf("get transactions by cursor: %w", err)
	}

	transactionResponses := make([]dto.TransactionsResponse, 0, len(transactions))
	for _, transaction := range transactions {
		transactionResponse := helper.ConvertToResponseType(transaction).(dto.TransactionsResponse)
		transactionResponses = append(transactionResponses, transactionResponse)
	}

	return transactionResponses, total, nil
}

// updateWalletBalance adjusts wallet balance based on category type and direction.
// direction: +1 means add amount to wallet, -1 means subtract.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) updateWalletBalance(ctx context.Context, walletID string, categoryType string, amount float64, direction int) error {
	wallet, err := transaction_serv.walletClient.GetWalletByID(ctx, walletID)
	if err != nil {
		return fmt.Errorf(errWalletNotFound, walletID, err)
	}

	switch categoryType {
	case "expense":
		wallet.Balance += float64(direction) * amount
	case "income":
		wallet.Balance -= float64(direction) * amount
	default:
		return fmt.Errorf(errInvalidTransactionType, categoryType)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, wallet); err != nil {
		return fmt.Errorf("update wallet balance [wallet_id=%s]: %w", walletID, err)
	}
	return nil
}

// applyBalanceForCreate adjusts wallet balance when creating a transaction.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) applyBalanceForCreate(ctx context.Context, transaction dto.TransactionsRequest, categoryType string) error {
	wallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transaction.WalletID)
	if err != nil {
		return fmt.Errorf(errWalletNotFound, transaction.WalletID, err)
	}

	switch categoryType {
	case "expense":
		if wallet.GetBalance() < transaction.Amount {
			return fmt.Errorf("insufficient wallet balance [wallet_id=%s]", transaction.WalletID)
		}
		wallet.Balance -= transaction.Amount
	case "income":
		wallet.Balance += transaction.Amount
	default:
		return fmt.Errorf(errInvalidTransactionType, categoryType)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, wallet); err != nil {
		return fmt.Errorf("update wallet balance [wallet_id=%s]: %w", transaction.WalletID, err)
	}
	return nil
}

func (transaction_serv *transactionsService) CreateTransaction(ctx context.Context, transaction dto.TransactionsRequest) (dto.TransactionsResponse, error) {
	category, err := transaction_serv.categoryRepo.GetCategoryByID(ctx, nil, transaction.CategoryID)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("category not found [id=%s]: %w", transaction.CategoryID, err)
	}

	tx, err := transaction_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("create transaction: begin transaction: %w", err)
	}

	defer tx.Rollback()

	CategoryID, err := helper.ParseUUID(transaction.CategoryID)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("invalid category id [id=%s]: %w", transaction.CategoryID, err)
	}

	WalletID, err := helper.ParseUUID(transaction.WalletID)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("invalid wallet id [id=%s]: %w", transaction.WalletID, err)
	}

	if !transaction.IsWalletNotCreated {
		if err := transaction_serv.applyBalanceForCreate(ctx, transaction, string(category.Type)); err != nil {
			return dto.TransactionsResponse{}, err
		}
	}

	transactionNew, err := transaction_serv.transactionRepo.CreateTransaction(ctx, tx, model.Transactions{
		WalletID:        WalletID,
		CategoryID:      CategoryID,
		Amount:          transaction.Amount,
		TransactionDate: transaction.Date,
		Description:     transaction.Description,
		Category:        category,
	})
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("create transaction: insert to db: %w", err)
	}

	if err := transaction_serv.processAttachments(ctx, tx, transactionNew.ID.String(), transaction.Attachments); err != nil {
		return dto.TransactionsResponse{}, err
	}

	transactionResponse := helper.ConvertToResponseType(transactionNew).(dto.TransactionsResponse)

	if err := transaction_serv.createOutboxMessage(ctx, tx, transactionResponse, data.OUTBOX_EVENT_TRANSACTION_CREATED); err != nil {
		return dto.TransactionsResponse{}, err
	}

	if err := tx.Commit(); err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("create transaction: commit: %w", err)
	}

	return transactionResponse, nil
}

// processAttachments handles uploading attachments during create.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) processAttachments(ctx context.Context, tx repository.Transaction, transactionID string, attachments []dto.UpdateAttachmentsRequest) error {
	for _, attachment := range attachments {
		if len(attachment.Files) == 0 {
			break
		}
		if _, err := transaction_serv.UploadAttachment(ctx, tx, transactionID, attachment.Files); err != nil {
			return fmt.Errorf("failed to upload attachment: %w", err)
		}
	}
	return nil
}

// createOutboxMessage marshals a transaction response and persists it to the outbox.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) createOutboxMessage(ctx context.Context, tx repository.Transaction, response dto.TransactionsResponse, eventType string) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("marshal transaction response: %w", err)
	}

	outboxMsg := &model.OutboxMessage{
		AggregateID: response.ID,
		EventType:   eventType,
		Payload:     payload,
		Published:   false,
		MaxRetries:  data.OUTBOX_PUBLISH_MAX_RETRIES,
	}

	return transaction_serv.outboxRepository.Create(ctx, tx, outboxMsg)
}

func (transaction_serv *transactionsService) FundTransfer(ctx context.Context, transaction dto.FundTransferRequest) (dto.FundTransferResponse, error) {
	tx, err := transaction_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("fund transfer: begin transaction: %w", err)
	}

	defer tx.Rollback()

	fromWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transaction.FromWalletID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("source wallet not found [id=%s]: %w", transaction.FromWalletID, err)
	}

	toWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transaction.ToWalletID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("destination wallet not found [id=%s]: %w", transaction.ToWalletID, err)
	}

	if fromWallet.GetBalance() < (transaction.Amount + transaction.AdminFee) {
		return dto.FundTransferResponse{}, fmt.Errorf("insufficient wallet balance [wallet_id=%s]", transaction.FromWalletID)
	}

	if fromWallet.GetId() == toWallet.GetId() {
		return dto.FundTransferResponse{}, fmt.Errorf("source wallet and destination wallet cannot be the same [wallet_id=%s]", transaction.FromWalletID)
	}

	fromWallet.Balance -= (transaction.Amount + transaction.AdminFee)
	toWallet.Balance += transaction.Amount

	FromWalletID, err := helper.ParseUUID(transaction.FromWalletID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("invalid from wallet id [id=%s]: %w", transaction.FromWalletID, err)
	}

	ToWalletID, err := helper.ParseUUID(transaction.ToWalletID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("invalid to wallet id [id=%s]: %w", transaction.ToWalletID, err)
	}

	FromCategoryID, err := helper.ParseUUID(transaction.CashOutCategoryID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("invalid from category id [id=%s]: %w", transaction.CashOutCategoryID, err)
	}

	ToCategoryID, err := helper.ParseUUID(transaction.CashInCategoryID)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("invalid to category id [id=%s]: %w", transaction.CashInCategoryID, err)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, fromWallet); err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("update from wallet balance: %w", err)
	}
	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, toWallet); err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("update to wallet balance: %w", err)
	}

	transactionNewFrom, err := transaction_serv.transactionRepo.CreateTransaction(ctx, tx, model.Transactions{
		WalletID:        FromWalletID,
		CategoryID:      FromCategoryID,
		Amount:          transaction.Amount + transaction.AdminFee,
		TransactionDate: transaction.Date,
		Description:     "fund transfer to " + toWallet.GetName() + "(Cash Out)",
	})
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("create from transaction: insert to db: %w", err)
	}

	transactionNewTo, err := transaction_serv.transactionRepo.CreateTransaction(ctx, tx, model.Transactions{
		WalletID:        ToWalletID,
		CategoryID:      ToCategoryID,
		Amount:          transaction.Amount,
		TransactionDate: transaction.Date,
		Description:     "fund transfer from " + fromWallet.GetName() + "(Cash In)",
	})
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("create to transaction: insert to db: %w", err)
	}

	fromResponse := helper.ConvertToResponseType(transactionNewFrom).(dto.TransactionsResponse)
	toResponse := helper.ConvertToResponseType(transactionNewTo).(dto.TransactionsResponse)

	if err := transaction_serv.createOutboxMessage(ctx, tx, fromResponse, data.OUTBOX_EVENT_TRANSACTION_CREATED); err != nil {
		return dto.FundTransferResponse{}, err
	}

	if err := transaction_serv.createOutboxMessage(ctx, tx, toResponse, data.OUTBOX_EVENT_TRANSACTION_CREATED); err != nil {
		return dto.FundTransferResponse{}, err
	}

	if err := tx.Commit(); err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("fund transfer: commit: %w", err)
	}

	return dto.FundTransferResponse{
		CashOutTransactionID: transactionNewFrom.ID.String(),
		CashInTransactionID:  transactionNewTo.ID.String(),
		FromWalletID:         transaction.FromWalletID,
		ToWalletID:           transaction.ToWalletID,
		Amount:               transaction.Amount,
		Date:                 transaction.Date,
		Description:          transaction.Description,
	}, nil
}

func (transaction_serv *transactionsService) UploadAttachment(ctx context.Context, tx repository.Transaction, transactionID string, files []string) ([]dto.AttachmentsResponse, error) {
	var attachmentResponses []dto.AttachmentsResponse

	if transactionID == "" {
		return nil, fmt.Errorf("transaction ID is required")
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to upload")
	}

	for idx, file := range files {
		if file == "" {
			return nil, fmt.Errorf("file is empty [index=%d]", idx)
		}

		// Use the passed ctx instead of context.Background() (godre:S8239)
		fileReq := miniofs.UploadRequest{
			Prefix:     fmt.Sprintf("%s_%s", miniofs.TRANSACTION_ATTACHMENT_PREFIX, transactionID),
			Base64Data: file,
			BucketName: miniofs.TRANSACTION_ATTACHMENT_BUCKET,
			Validation: miniofs.CreateImageValidationConfig(),
		}
		res, err := transaction_serv.minio.UploadFile(ctx, fileReq)
		if err != nil {
			return nil, fmt.Errorf("upload file %d [transaction_id=%s]: %w", idx+1, transactionID, err)
		}

		TransactionUUID, err := uuid.Parse(transactionID)
		if err != nil {
			return nil, fmt.Errorf("invalid transaction id [id=%s]: %w", transactionID, err)
		}

		attachment, err := transaction_serv.attachmentRepo.CreateAttachment(ctx, tx, model.Attachments{
			Image:         miniofs.ReplaceURL(res.URL),
			TransactionID: TransactionUUID,
			Size:          res.Size,
			Format:        res.Ext,
		})
		if err != nil {
			return nil, fmt.Errorf("create attachment [transaction_id=%s]: %w", transactionID, err)
		}

		attachmentResponses = append(attachmentResponses, dto.AttachmentsResponse{
			ID:            attachment.ID.String(),
			Image:         attachment.Image,
			TransactionID: attachment.TransactionID.String(),
			CreatedAt:     attachment.CreatedAt.String(),
		})
	}

	return attachmentResponses, nil
}

// applyBalanceForWalletChange updates old and new wallet balances when changing wallet on update.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) applyBalanceForWalletChange(ctx context.Context, transactionExist model.Transactions, newWalletID string, newAmount float64) (model.Transactions, error) {
	oldWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transactionExist.WalletID.String())
	if err != nil {
		return transactionExist, fmt.Errorf(errWalletNotFound, transactionExist.WalletID.String(), err)
	}

	switch transactionExist.Category.Type {
	case "expense":
		oldWallet.Balance += transactionExist.Amount
	case "income":
		oldWallet.Balance -= transactionExist.Amount
	default:
		return transactionExist, fmt.Errorf(errInvalidTransactionType, transactionExist.Category.Type)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, oldWallet); err != nil {
		return transactionExist, fmt.Errorf("update old wallet balance: %w", err)
	}

	newWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, newWalletID)
	if err != nil {
		return transactionExist, fmt.Errorf("new wallet not found [id=%s]: %w", newWalletID, err)
	}

	switch transactionExist.Category.Type {
	case "expense":
		newWallet.Balance -= newAmount
	case "income":
		newWallet.Balance += newAmount
	default:
		return transactionExist, fmt.Errorf(errInvalidTransactionType, transactionExist.Category.Type)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, newWallet); err != nil {
		return transactionExist, fmt.Errorf("update new wallet balance: %w", err)
	}

	walletUUID, err := helper.ParseUUID(newWalletID)
	if err != nil {
		return transactionExist, fmt.Errorf("invalid wallet id [id=%s]: %w", newWalletID, err)
	}
	transactionExist.WalletID = walletUUID
	return transactionExist, nil
}

// applyBalanceForAmountChange updates wallet balance when only the amount changes.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) applyBalanceForAmountChange(ctx context.Context, transactionExist model.Transactions, newAmount float64) error {
	oldWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transactionExist.WalletID.String())
	if err != nil {
		return fmt.Errorf(errWalletNotFound, transactionExist.WalletID.String(), err)
	}

	switch transactionExist.Category.Type {
	case "expense":
		oldWallet.Balance += transactionExist.Amount
		oldWallet.Balance -= newAmount
	case "income":
		oldWallet.Balance -= transactionExist.Amount
		oldWallet.Balance += newAmount
	default:
		return fmt.Errorf(errInvalidTransactionType, transactionExist.Category.Type)
	}

	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, oldWallet); err != nil {
		return fmt.Errorf("update wallet balance: %w", err)
	}
	return nil
}

// handleUpdateAttachments processes attachment create/delete actions during update.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) handleUpdateAttachments(ctx context.Context, tx repository.Transaction, transactionID model.Transactions, attachments []dto.UpdateAttachmentsRequest) error {
	for _, attachment := range attachments {
		switch attachment.Status {
		case "create":
			if len(attachment.Files) == 0 {
				return fmt.Errorf("no files to upload")
			}
			if _, err := transaction_serv.UploadAttachment(ctx, tx, transactionID.ID.String(), attachment.Files); err != nil {
				return fmt.Errorf("failed to upload attachment: %w", err)
			}

		case "delete":
			if len(attachment.Files) == 0 {
				return fmt.Errorf("no files to delete")
			}
			if err := transaction_serv.deleteAttachments(ctx, tx, transactionID, attachment.Files); err != nil {
				return err
			}

		default:
			return fmt.Errorf("invalid attachment status [status=%s]", attachment.Status)
		}
	}
	return nil
}

// deleteAttachments deletes a list of attachments by ID, verifying ownership.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) deleteAttachments(ctx context.Context, tx repository.Transaction, transactionUpdated model.Transactions, fileIDs []string) error {
	for _, ID := range fileIDs {
		attachmentToDelete, err := transaction_serv.attachmentRepo.GetAttachmentByID(ctx, tx, ID)
		if err != nil {
			return fmt.Errorf("attachment with file %s not found: %w", ID, err)
		}

		if attachmentToDelete.TransactionID != transactionUpdated.ID {
			return fmt.Errorf("attachment with file %s does not belong to transaction %s", ID, transactionUpdated.ID)
		}

		if _, err := transaction_serv.attachmentRepo.DeleteAttachment(ctx, tx, attachmentToDelete); err != nil {
			return fmt.Errorf("attachment with file %v not found: %w", attachmentToDelete, err)
		}
	}
	return nil
}

func (transaction_serv *transactionsService) UpdateTransaction(ctx context.Context, id string, transaction dto.TransactionsRequest) (dto.TransactionsResponse, error) {
	tx, err := transaction_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("update transaction: begin transaction: %w", err)
	}

	defer tx.Rollback()

	transactionExist, err := transaction_serv.transactionRepo.GetTransactionByID(ctx, tx, id)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf(errTransactionNotFound, id, err)
	}

	if transaction.CategoryID != transactionExist.CategoryID.String() {
		_, err = transaction_serv.categoryRepo.GetCategoryByID(ctx, tx, transaction.CategoryID)
		if err != nil {
			return dto.TransactionsResponse{}, fmt.Errorf("category not found [id=%s]: %w", transaction.CategoryID, err)
		}

		CategoryID, err := helper.ParseUUID(transaction.CategoryID)
		if err != nil {
			return dto.TransactionsResponse{}, fmt.Errorf("invalid category id [id=%s]: %w", transaction.CategoryID, err)
		}

		transactionExist.CategoryID = CategoryID
	}

	if transaction.WalletID != transactionExist.WalletID.String() {
		transactionExist, err = transaction_serv.applyBalanceForWalletChange(ctx, transactionExist, transaction.WalletID, transaction.Amount)
		if err != nil {
			return dto.TransactionsResponse{}, err
		}
	}

	if transaction.Amount != transactionExist.Amount {
		if err := transaction_serv.applyBalanceForAmountChange(ctx, transactionExist, transaction.Amount); err != nil {
			return dto.TransactionsResponse{}, err
		}
		transactionExist.Amount = transaction.Amount
	}

	if !transaction.Date.IsZero() && !utils.SameDate(transaction.Date, transactionExist.TransactionDate) {
		transactionExist.TransactionDate = transaction.Date
	}

	if transaction.Description != "" {
		transactionExist.Description = transaction.Description
	}

	transactionUpdated, err := transaction_serv.transactionRepo.UpdateTransaction(ctx, tx, transactionExist)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("update transaction [id=%s]: update in db: %w", id, err)
	}

	if len(transaction.Attachments) > 0 {
		if err := transaction_serv.handleUpdateAttachments(ctx, tx, transactionUpdated, transaction.Attachments); err != nil {
			return dto.TransactionsResponse{}, err
		}
	}

	transactionResponse := helper.ConvertToResponseType(transactionUpdated).(dto.TransactionsResponse)

	if err := transaction_serv.createOutboxMessage(ctx, tx, transactionResponse, data.OUTBOX_EVENT_TRANSACTION_UPDATED); err != nil {
		return dto.TransactionsResponse{}, err
	}

	if err = tx.Commit(); err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("update transaction: commit: %w", err)
	}

	return transactionResponse, nil
}

// resolveWalletBalanceForDelete calculates the balance adjustment needed when deleting a transaction.
// Extracted to reduce cognitive complexity (go:S3776).
func resolveWalletBalanceAdjustment(categoryType string, categoryName string, amount float64, balance float64) (float64, error) {
	switch categoryType {
	case "expense":
		return balance + amount, nil
	case "income":
		return balance - amount, nil
	default:
		switch categoryName {
		case "Cash Out":
			return balance + amount, nil
		case "Cash In":
			return balance - amount, nil
		default:
			return 0, fmt.Errorf(errInvalidTransactionType, categoryType)
		}
	}
}

func (transaction_serv *transactionsService) DeleteTransaction(ctx context.Context, id string) (dto.TransactionsResponse, error) {
	tx, err := transaction_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("delete transaction: begin transaction: %w", err)
	}

	defer tx.Rollback()

	transactionExist, err := transaction_serv.transactionRepo.GetTransactionByID(ctx, tx, id)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf(errTransactionNotFound, id, err)
	}

	wallet, err := transaction_serv.walletClient.GetWalletByID(ctx, transactionExist.WalletID.String())
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf(errWalletNotFound, transactionExist.WalletID.String(), err)
	}

	newBalance, err := resolveWalletBalanceAdjustment(
		string(transactionExist.Category.Type),
		transactionExist.Category.Name,
		transactionExist.Amount,
		wallet.Balance,
	)
	if err != nil {
		return dto.TransactionsResponse{}, err
	}

	wallet.Balance = newBalance
	if _, err = transaction_serv.walletClient.UpdateWallet(ctx, wallet); err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("update wallet balance: %w", err)
	}

	transactionDeleted, err := transaction_serv.transactionRepo.DeleteTransaction(ctx, tx, transactionExist)
	if err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("delete transaction [id=%s]: delete from db: %w", id, err)
	}

	transactionResponse := helper.ConvertToResponseType(transactionDeleted).(dto.TransactionsResponse)

	if err := transaction_serv.createOutboxMessage(ctx, tx, transactionResponse, data.OUTBOX_EVENT_TRANSACTION_DELETED); err != nil {
		return dto.TransactionsResponse{}, err
	}

	if err := tx.Commit(); err != nil {
		return dto.TransactionsResponse{}, fmt.Errorf("delete transaction: commit: %w", err)
	}

	return transactionResponse, nil
}