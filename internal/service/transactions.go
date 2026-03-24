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

	wpb "github.com/MuhammadMiftaa/Refina-Protobuf/wallet"
	"github.com/google/uuid"
)

// Error message constants to avoid duplication (go:S1192)
const (
	errTransactionNotFound    = "transaction not found [id=%s]: %w"
	errWalletNotFound         = "wallet not found [id=%s]: %w"
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

// walletPair holds both wallets for a fund transfer operation.
type walletPair struct {
	from *wpb.Wallet
	to   *wpb.Wallet
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

// validateFundTransferWallets fetches both wallets and validates balance and same-wallet constraint.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) validateFundTransferWallets(ctx context.Context, req dto.FundTransferRequest) (*walletPair, error) {
	fromWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, req.FromWalletID)
	if err != nil {
		return nil, fmt.Errorf("source wallet not found [id=%s]: %w", req.FromWalletID, err)
	}

	toWallet, err := transaction_serv.walletClient.GetWalletByID(ctx, req.ToWalletID)
	if err != nil {
		return nil, fmt.Errorf("destination wallet not found [id=%s]: %w", req.ToWalletID, err)
	}

	if fromWallet.GetBalance() < (req.Amount + req.AdminFee) {
		return nil, fmt.Errorf("insufficient wallet balance [wallet_id=%s]", req.FromWalletID)
	}

	if fromWallet.GetId() == toWallet.GetId() {
		return nil, fmt.Errorf("source wallet and destination wallet cannot be the same [wallet_id=%s]", req.FromWalletID)
	}

	return &walletPair{from: fromWallet, to: toWallet}, nil
}

// updateFundTransferBalances updates balances for both wallets in a fund transfer.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) updateFundTransferBalances(ctx context.Context, req dto.FundTransferRequest, pair *walletPair) error {
	pair.from.Balance -= (req.Amount + req.AdminFee)
	pair.to.Balance += req.Amount

	if _, err := transaction_serv.walletClient.UpdateWallet(ctx, pair.from); err != nil {
		return fmt.Errorf("update from wallet balance: %w", err)
	}
	if _, err := transaction_serv.walletClient.UpdateWallet(ctx, pair.to); err != nil {
		return fmt.Errorf("update to wallet balance: %w", err)
	}
	return nil
}

// fundTransferIDs holds parsed UUIDs for a fund transfer.
type fundTransferIDs struct {
	fromWallet uuid.UUID
	toWallet   uuid.UUID
	fromCat    uuid.UUID
	toCat      uuid.UUID
}

// parseFundTransferIDs parses and validates all UUIDs needed for fund transfer.
// Extracted to reduce cognitive complexity (go:S3776).
func parseFundTransferIDs(req dto.FundTransferRequest) (fundTransferIDs, error) {
	ids := fundTransferIDs{}
	var err error

	ids.fromWallet, err = helper.ParseUUID(req.FromWalletID)
	if err != nil {
		return ids, fmt.Errorf("invalid from wallet id [id=%s]: %w", req.FromWalletID, err)
	}

	ids.toWallet, err = helper.ParseUUID(req.ToWalletID)
	if err != nil {
		return ids, fmt.Errorf("invalid to wallet id [id=%s]: %w", req.ToWalletID, err)
	}

	ids.fromCat, err = helper.ParseUUID(req.CashOutCategoryID)
	if err != nil {
		return ids, fmt.Errorf("invalid from category id [id=%s]: %w", req.CashOutCategoryID, err)
	}

	ids.toCat, err = helper.ParseUUID(req.CashInCategoryID)
	if err != nil {
		return ids, fmt.Errorf("invalid to category id [id=%s]: %w", req.CashInCategoryID, err)
	}

	return ids, nil
}

// createFundTransferTransactions creates both cash-out and cash-in transactions.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) createFundTransferTransactions(ctx context.Context, tx repository.Transaction, req dto.FundTransferRequest, pair *walletPair, ids fundTransferIDs) (model.Transactions, model.Transactions, error) {
	cashOutTxn, err := transaction_serv.transactionRepo.CreateTransaction(ctx, tx, model.Transactions{
		WalletID:        ids.fromWallet,
		CategoryID:      ids.fromCat,
		Amount:          req.Amount + req.AdminFee,
		TransactionDate: req.Date,
		Description:     "fund transfer to " + pair.to.GetName() + "(Cash Out)",
	})
	if err != nil {
		return model.Transactions{}, model.Transactions{}, fmt.Errorf("create from transaction: insert to db: %w", err)
	}

	cashInTxn, err := transaction_serv.transactionRepo.CreateTransaction(ctx, tx, model.Transactions{
		WalletID:        ids.toWallet,
		CategoryID:      ids.toCat,
		Amount:          req.Amount,
		TransactionDate: req.Date,
		Description:     "fund transfer from " + pair.from.GetName() + "(Cash In)",
	})
	if err != nil {
		return model.Transactions{}, model.Transactions{}, fmt.Errorf("create to transaction: insert to db: %w", err)
	}

	return cashOutTxn, cashInTxn, nil
}

func (transaction_serv *transactionsService) FundTransfer(ctx context.Context, transaction dto.FundTransferRequest) (dto.FundTransferResponse, error) {
	tx, err := transaction_serv.txManager.Begin(ctx)
	if err != nil {
		return dto.FundTransferResponse{}, fmt.Errorf("fund transfer: begin transaction: %w", err)
	}
	defer tx.Rollback()

	pair, err := transaction_serv.validateFundTransferWallets(ctx, transaction)
	if err != nil {
		return dto.FundTransferResponse{}, err
	}

	ids, err := parseFundTransferIDs(transaction)
	if err != nil {
		return dto.FundTransferResponse{}, err
	}

	if err := transaction_serv.updateFundTransferBalances(ctx, transaction, pair); err != nil {
		return dto.FundTransferResponse{}, err
	}

	cashOutTxn, cashInTxn, err := transaction_serv.createFundTransferTransactions(ctx, tx, transaction, pair, ids)
	if err != nil {
		return dto.FundTransferResponse{}, err
	}

	fromResponse := helper.ConvertToResponseType(cashOutTxn).(dto.TransactionsResponse)
	toResponse := helper.ConvertToResponseType(cashInTxn).(dto.TransactionsResponse)

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
		CashOutTransactionID: cashOutTxn.ID.String(),
		CashInTransactionID:  cashInTxn.ID.String(),
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

// applyUpdateCategoryChange handles category change during UpdateTransaction.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) applyUpdateCategoryChange(ctx context.Context, tx repository.Transaction, transactionExist *model.Transactions, newCategoryID string) error {
	if newCategoryID == transactionExist.CategoryID.String() {
		return nil
	}

	if _, err := transaction_serv.categoryRepo.GetCategoryByID(ctx, tx, newCategoryID); err != nil {
		return fmt.Errorf("category not found [id=%s]: %w", newCategoryID, err)
	}

	categoryID, err := helper.ParseUUID(newCategoryID)
	if err != nil {
		return fmt.Errorf("invalid category id [id=%s]: %w", newCategoryID, err)
	}
	transactionExist.CategoryID = categoryID
	return nil
}

// applyUpdateWalletAndAmountChange handles wallet/amount changes during UpdateTransaction.
// Extracted to reduce cognitive complexity (go:S3776).
func (transaction_serv *transactionsService) applyUpdateWalletAndAmountChange(ctx context.Context, transactionExist *model.Transactions, req dto.TransactionsRequest) error {
	if req.WalletID != transactionExist.WalletID.String() {
		updated, err := transaction_serv.applyBalanceForWalletChange(ctx, *transactionExist, req.WalletID, req.Amount)
		if err != nil {
			return err
		}
		*transactionExist = updated
	}

	if req.Amount != transactionExist.Amount {
		if err := transaction_serv.applyBalanceForAmountChange(ctx, *transactionExist, req.Amount); err != nil {
			return err
		}
		transactionExist.Amount = req.Amount
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

	if err := transaction_serv.applyUpdateCategoryChange(ctx, tx, &transactionExist, transaction.CategoryID); err != nil {
		return dto.TransactionsResponse{}, err
	}

	if err := transaction_serv.applyUpdateWalletAndAmountChange(ctx, &transactionExist, transaction); err != nil {
		return dto.TransactionsResponse{}, err
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

// resolveWalletBalanceAdjustment calculates the balance adjustment needed when deleting a transaction.
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