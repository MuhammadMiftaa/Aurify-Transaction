package server

import (
	"context"
	"fmt"
	"time"

	"refina-transaction/config/log"
	"refina-transaction/interface/grpc/interceptor"
	"refina-transaction/internal/service"
	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/utils/data"

	tpb "github.com/MuhammadMiftaa/Refina-Protobuf/transaction"
)

type transactionServer struct {
	tpb.UnimplementedTransactionServiceServer
	transactionService service.TransactionsService
	categoryService    service.CategoriesService
	attachmentService  service.AttachmentsService
}

// ──────────────────────────────────────────────────────────────────────────────
// Transaction RPCs
// ──────────────────────────────────────────────────────────────────────────────

func (s *transactionServer) GetTransactions(req *tpb.GetTransactionOptions, stream tpb.TransactionService_GetTransactionsServer) error {
	ctx := stream.Context()

	userID := interceptor.UserIDFromContext(ctx)
	log.Debug("GetTransactions called", map[string]any{
		"service": data.GRPCServerService,
		"user_id": userID,
		"limit":   req.GetLimit(),
	})

	transactions, err := s.transactionService.GetAllTransactions(ctx)
	if err != nil {
		log.Error(data.LogGetTransactionsFailed, map[string]any{
			"service": data.GRPCServerService,
			"error":   err.Error(),
		})
		return fmt.Errorf("get transactions: %w", err)
	}

	for _, txn := range transactions {
		if err := stream.Send(toProtoTransaction(txn)); err != nil {
			log.Error(data.LogStreamSendFailed, map[string]any{
				"service":        data.GRPCServerService,
				"transaction_id": txn.ID,
				"error":          err.Error(),
			})
			return fmt.Errorf("stream send [transaction_id=%s]: %w", txn.ID, err)
		}
	}
	return nil
}

func (s *transactionServer) GetUserTransactions(ctx context.Context, req *tpb.GetUserTransactionsRequest) (*tpb.GetUserTransactionsResponse, error) {
	userID := interceptor.UserIDFromContext(ctx)
	log.Debug("GetUserTransactions called", map[string]any{
		"service":    data.GRPCServerService,
		"user_id":    userID,
		"wallet_ids": req.GetWalletIds(),
		"page":       req.GetPage(),
		"page_size":  req.GetPageSize(),
	})

	// Fetch all transactions for the given wallet IDs via service layer
	allTransactions, err := s.transactionService.GetTransactionsByWalletIDs(ctx, req.GetWalletIds())
	if err != nil {
		log.Error(data.LogGetUserTransactionsFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("get user transactions: %w", err)
	}

	// ── Apply filters ──
	filtered := applyFilters(allTransactions, req)

	// ── Apply sorting ──
	applySorting(filtered, req.GetSortBy(), req.GetSortOrder())

	// ── Pagination ──
	total := int32(len(filtered))
	page := req.GetPage()
	pageSize := req.GetPageSize()
	if page <= 0 {
		page = 1
	}
	if pageSize < 0 {
		pageSize = 9999
	}
	totalPages := (total + pageSize - 1) / pageSize

	start := (page - 1) * pageSize
	end := start + pageSize
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	pageItems := filtered[start:end]

	// ── Build response ──
	protoTxns := make([]*tpb.TransactionDetail, 0, len(pageItems))
	for _, txn := range pageItems {
		protoTxns = append(protoTxns, toProtoTransactionDetail(txn))
	}

	log.Info(data.LogGetUserTransactionsSuccess, map[string]any{
		"service":     data.GRPCServerService,
		"user_id":     userID,
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": totalPages,
	})

	return &tpb.GetUserTransactionsResponse{
		Transactions: protoTxns,
		Total:        total,
		Page:         page,
		PageSize:     pageSize,
		TotalPages:   totalPages,
	}, nil
}

func (s *transactionServer) GetTransactionByID(ctx context.Context, req *tpb.TransactionID) (*tpb.TransactionDetail, error) {
	userID := interceptor.UserIDFromContext(ctx)

	txn, err := s.transactionService.GetTransactionByID(ctx, req.GetId())
	if err != nil {
		log.Error(data.LogGetTransactionByIDGRPCFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"transaction_id": req.GetId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("get transaction by id [id=%s]: %w", req.GetId(), err)
	}

	log.Info(data.LogGetTransactionByIDGRPCSuccess, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"transaction_id": req.GetId(),
	})

	return toProtoTransactionDetail(txn), nil
}

func (s *transactionServer) CreateTransaction(ctx context.Context, req *tpb.CreateTransactionRequest) (*tpb.TransactionDetail, error) {
	userID := interceptor.UserIDFromContext(ctx)

	transactionDate, err := time.Parse(time.RFC3339, req.GetTransactionDate())
	if err != nil {
		return nil, fmt.Errorf("create transaction: invalid date format: %w", err)
	}

	svcReq := dto.TransactionsRequest{
		WalletID:    req.GetWalletId(),
		CategoryID:  req.GetCategoryId(),
		Amount:      req.GetAmount(),
		Date:        transactionDate,
		Description: req.GetDescription(),
		Attachments: []dto.UpdateAttachmentsRequest{
			{
				Status: "create",
				Files:  req.GetAttachments(),
			},
		},
	}

	txn, err := s.transactionService.CreateTransaction(ctx, svcReq)
	if err != nil {
		log.Error(data.LogCreateTransactionFailed, map[string]any{
			"service":   data.GRPCServerService,
			"user_id":   userID,
			"wallet_id": req.GetWalletId(),
			"error":     err.Error(),
		})
		return nil, fmt.Errorf("create transaction: %w", err)
	}

	log.Info(data.LogTransactionCreated, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"transaction_id": txn.ID,
		"wallet_id":      txn.WalletID,
	})

	return toProtoTransactionDetail(txn), nil
}

func (s *transactionServer) CreateFundTransfer(ctx context.Context, req *tpb.CreateFundTransferRequest) (*tpb.FundTransferResponse, error) {
	userID := interceptor.UserIDFromContext(ctx)

	transactionDate, err := time.Parse(time.RFC3339, req.GetTransactionDate())
	if err != nil {
		return nil, fmt.Errorf("create fund transfer: invalid date format: %w", err)
	}

	svcReq := dto.FundTransferRequest{
		CashInCategoryID:  req.GetCashInCategoryId(),
		CashOutCategoryID: req.GetCashOutCategoryId(),
		FromWalletID:      req.GetFromWalletId(),
		ToWalletID:        req.GetToWalletId(),
		Amount:            req.GetAmount(),
		AdminFee:          req.GetAdminFee(),
		Date:              transactionDate,
		Description:       req.GetDescription(),
	}

	result, err := s.transactionService.FundTransfer(ctx, svcReq)
	if err != nil {
		log.Error(data.LogCreateFundTransferFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"from_wallet_id": req.GetFromWalletId(),
			"to_wallet_id":   req.GetToWalletId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("create fund transfer: %w", err)
	}

	log.Info(data.LogFundTransferCreated, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"from_wallet_id": result.FromWalletID,
		"to_wallet_id":   result.ToWalletID,
		"amount":         result.Amount,
	})

	return &tpb.FundTransferResponse{
		CashOutTransactionId: result.CashOutTransactionID,
		CashInTransactionId:  result.CashInTransactionID,
		FromWalletId:         result.FromWalletID,
		ToWalletId:           result.ToWalletID,
		Amount:               result.Amount,
		Date:                 result.Date.Format(time.RFC3339),
		Description:          result.Description,
	}, nil
}

func (s *transactionServer) UpdateTransaction(ctx context.Context, req *tpb.UpdateTransactionRequest) (*tpb.TransactionDetail, error) {
	userID := interceptor.UserIDFromContext(ctx)

	transactionDate, err := time.Parse(time.RFC3339, req.GetTransactionDate())
	if err != nil {
		return nil, fmt.Errorf("update transaction: invalid date format: %w", err)
	}

	// Convert proto attachment actions → service DTO
	attachmentActions := make([]dto.UpdateAttachmentsRequest, 0, len(req.GetAttachmentActions()))
	for _, action := range req.GetAttachmentActions() {
		attachmentActions = append(attachmentActions, dto.UpdateAttachmentsRequest{
			Status: action.GetStatus(),
			Files:  action.GetFiles(),
		})
	}

	svcReq := dto.TransactionsRequest{
		WalletID:    req.GetWalletId(),
		CategoryID:  req.GetCategoryId(),
		Amount:      req.GetAmount(),
		Date:        transactionDate,
		Description: req.GetDescription(),
		Attachments: attachmentActions,
	}

	txn, err := s.transactionService.UpdateTransaction(ctx, req.GetId(), svcReq)
	if err != nil {
		log.Error(data.LogUpdateTransactionGRPCFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"transaction_id": req.GetId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("update transaction [id=%s]: %w", req.GetId(), err)
	}

	log.Info(data.LogTransactionUpdated, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"transaction_id": txn.ID,
	})

	return toProtoTransactionDetail(txn), nil
}

func (s *transactionServer) DeleteTransaction(ctx context.Context, req *tpb.TransactionID) (*tpb.TransactionDetail, error) {
	userID := interceptor.UserIDFromContext(ctx)

	txn, err := s.transactionService.DeleteTransaction(ctx, req.GetId())
	if err != nil {
		log.Error(data.LogDeleteTransactionFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"transaction_id": req.GetId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("delete transaction [id=%s]: %w", req.GetId(), err)
	}

	log.Info(data.LogTransactionDeleted, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"transaction_id": req.GetId(),
	})

	return toProtoTransactionDetail(txn), nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Category RPCs
// ──────────────────────────────────────────────────────────────────────────────

func (s *transactionServer) GetCategories(ctx context.Context, req *tpb.GetCategoriesRequest) (*tpb.GetCategoriesResponse, error) {
	userID := interceptor.UserIDFromContext(ctx)
	filterType := req.GetType()

	var categoryGroups []*tpb.CategoryGroup

	if filterType != "" {
		// Filter by type — use GetCategoriesByType which returns view structs
		viewCategories, err := s.categoryService.GetCategoriesByType(ctx, filterType)
		if err != nil {
			log.Error(data.LogGetCategoriesGRPCFailed, map[string]any{
				"service": data.GRPCServerService,
				"user_id": userID,
				"type":    filterType,
				"error":   err.Error(),
			})
			return nil, fmt.Errorf("get categories by type [type=%s]: %w", filterType, err)
		}
		for _, vg := range viewCategories {
			items := make([]*tpb.CategoryItem, 0, len(vg.Category))
			for _, c := range vg.Category {
				items = append(items, &tpb.CategoryItem{
					Id:   c.ID,
					Name: c.Name,
				})
			}
			categoryGroups = append(categoryGroups, &tpb.CategoryGroup{
				GroupName:  vg.GroupName,
				Type:       vg.Type,
				Categories: items,
			})
		}
	} else {
		// Get all categories grouped by parent
		categories, err := s.categoryService.GetAllCategories(ctx)
		if err != nil {
			log.Error(data.LogGetCategoriesGRPCFailed, map[string]any{
				"service": data.GRPCServerService,
				"user_id": userID,
				"error":   err.Error(),
			})
			return nil, fmt.Errorf("get all categories: %w", err)
		}
		for _, cg := range categories {
			items := make([]*tpb.CategoryItem, 0, len(cg.Category))
			for _, c := range cg.Category {
				items = append(items, &tpb.CategoryItem{
					Id:   c.ID,
					Name: c.Name,
				})
			}
			categoryGroups = append(categoryGroups, &tpb.CategoryGroup{
				GroupName:  cg.GroupName,
				Type:       string(cg.Type),
				Categories: items,
			})
		}
	}

	log.Info(data.LogGetCategoriesGRPCSuccess, map[string]any{
		"service": data.GRPCServerService,
		"user_id": userID,
		"type":    filterType,
		"count":   len(categoryGroups),
	})

	return &tpb.GetCategoriesResponse{
		Categories: categoryGroups,
	}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Attachment RPCs
// ──────────────────────────────────────────────────────────────────────────────

func (s *transactionServer) GetAttachmentsByTransactionID(ctx context.Context, req *tpb.TransactionID) (*tpb.GetAttachmentsResponse, error) {
	userID := interceptor.UserIDFromContext(ctx)

	attachments, err := s.attachmentService.GetAttachmentsByTransactionID(ctx, req.GetId())
	if err != nil {
		log.Error(data.LogGetAttachmentsByTxnIDFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"transaction_id": req.GetId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("get attachments by transaction id [id=%s]: %w", req.GetId(), err)
	}

	protoAttachments := make([]*tpb.Attachment, 0, len(attachments))
	for _, a := range attachments {
		protoAttachments = append(protoAttachments, toProtoAttachment(a))
	}

	log.Info(data.LogGetAttachmentsByTxnIDSuccess, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"transaction_id": req.GetId(),
		"count":          len(protoAttachments),
	})

	return &tpb.GetAttachmentsResponse{
		Attachments: protoAttachments,
	}, nil
}

func (s *transactionServer) CreateAttachment(ctx context.Context, req *tpb.CreateAttachmentRequest) (*tpb.Attachment, error) {
	userID := interceptor.UserIDFromContext(ctx)

	svcReq := dto.AttachmentsRequest{
		TransactionID: req.GetTransactionId(),
		Image:         req.GetImage(),
	}

	attachment, err := s.attachmentService.CreateAttachment(ctx, svcReq)
	if err != nil {
		log.Error(data.LogCreateAttachmentGRPCFailed, map[string]any{
			"service":        data.GRPCServerService,
			"user_id":        userID,
			"transaction_id": req.GetTransactionId(),
			"error":          err.Error(),
		})
		return nil, fmt.Errorf("create attachment: %w", err)
	}

	log.Info(data.LogAttachmentCreated, map[string]any{
		"service":        data.GRPCServerService,
		"user_id":        userID,
		"attachment_id":  attachment.ID,
		"transaction_id": attachment.TransactionID,
	})

	return toProtoAttachment(attachment), nil
}

func (s *transactionServer) DeleteAttachment(ctx context.Context, req *tpb.AttachmentID) (*tpb.Attachment, error) {
	userID := interceptor.UserIDFromContext(ctx)

	attachment, err := s.attachmentService.DeleteAttachment(ctx, req.GetId())
	if err != nil {
		log.Error(data.LogDeleteAttachmentGRPCFailed, map[string]any{
			"service":       data.GRPCServerService,
			"user_id":       userID,
			"attachment_id": req.GetId(),
			"error":         err.Error(),
		})
		return nil, fmt.Errorf("delete attachment [id=%s]: %w", req.GetId(), err)
	}

	log.Info(data.LogAttachmentDeleted, map[string]any{
		"service":       data.GRPCServerService,
		"user_id":       userID,
		"attachment_id": req.GetId(),
	})

	return toProtoAttachment(attachment), nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Proto Converters
// ──────────────────────────────────────────────────────────────────────────────

func toProtoTransaction(txn dto.TransactionsResponse) *tpb.Transaction {
	return &tpb.Transaction{
		Id:              txn.ID,
		WalletId:        txn.WalletID,
		Amount:          txn.Amount,
		CategoryId:      txn.CategoryID,
		CategoryName:    txn.CategoryName,
		CategoryType:    txn.CategoryType,
		TransactionDate: txn.TransactionDate.Format(time.RFC3339),
		Description:     txn.Description,
	}
}

func toProtoTransactionDetail(txn dto.TransactionsResponse) *tpb.TransactionDetail {
	protoAttachments := make([]*tpb.Attachment, 0, len(txn.Attachments))
	for _, a := range txn.Attachments {
		protoAttachments = append(protoAttachments, toProtoAttachment(a))
	}

	return &tpb.TransactionDetail{
		Id:              txn.ID,
		WalletId:        txn.WalletID,
		CategoryId:      txn.CategoryID,
		CategoryName:    txn.CategoryName,
		CategoryType:    txn.CategoryType,
		Amount:          txn.Amount,
		TransactionDate: txn.TransactionDate.Format(time.RFC3339),
		Description:     txn.Description,
		Attachments:     protoAttachments,
	}
}

func toProtoAttachment(a dto.AttachmentsResponse) *tpb.Attachment {
	return &tpb.Attachment{
		Id:            a.ID,
		TransactionId: a.TransactionID,
		Image:         a.Image,
		Format:        a.Format,
		Size:          a.Size,
		CreatedAt:     a.CreatedAt,
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Filtering & Sorting helpers
// ──────────────────────────────────────────────────────────────────────────────

func applyFilters(transactions []dto.TransactionsResponse, req *tpb.GetUserTransactionsRequest) []dto.TransactionsResponse {
	filtered := make([]dto.TransactionsResponse, 0, len(transactions))

	for _, txn := range transactions {
		// Wallet ID filter
		if req.GetWalletId() != "" && txn.WalletID != req.GetWalletId() {
			continue
		}

		// Category ID filter
		if req.GetCategoryId() != "" && txn.CategoryID != req.GetCategoryId() {
			continue
		}

		// Category type filter
		if req.GetCategoryType() != "" && txn.CategoryType != req.GetCategoryType() {
			continue
		}

		// Date range filter
		if req.GetDateFrom() != "" {
			dateFrom, err := time.Parse(time.RFC3339, req.GetDateFrom())
			if err == nil && txn.TransactionDate.Before(dateFrom) {
				continue
			}
		}
		if req.GetDateTo() != "" {
			dateTo, err := time.Parse(time.RFC3339, req.GetDateTo())
			if err == nil && txn.TransactionDate.After(dateTo) {
				continue
			}
		}

		// Search filter (description)
		if req.GetSearch() != "" {
			search := req.GetSearch()
			if !containsIgnoreCase(txn.Description, search) && !containsIgnoreCase(txn.CategoryName, search) {
				continue
			}
		}

		filtered = append(filtered, txn)
	}

	return filtered
}

func applySorting(transactions []dto.TransactionsResponse, sortBy, sortOrder string) {
	if sortBy == "" {
		sortBy = "transaction_date"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}

	isDesc := sortOrder == "desc"

	// Simple sort using slices
	n := len(transactions)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			shouldSwap := false
			switch sortBy {
			case "amount":
				if isDesc {
					shouldSwap = transactions[j].Amount < transactions[j+1].Amount
				} else {
					shouldSwap = transactions[j].Amount > transactions[j+1].Amount
				}
			case "transaction_date":
				if isDesc {
					shouldSwap = transactions[j].TransactionDate.Before(transactions[j+1].TransactionDate)
				} else {
					shouldSwap = transactions[j].TransactionDate.After(transactions[j+1].TransactionDate)
				}
			default:
				// Default: sort by date desc
				if isDesc {
					shouldSwap = transactions[j].TransactionDate.Before(transactions[j+1].TransactionDate)
				} else {
					shouldSwap = transactions[j].TransactionDate.After(transactions[j+1].TransactionDate)
				}
			}
			if shouldSwap {
				transactions[j], transactions[j+1] = transactions[j+1], transactions[j]
			}
		}
	}
}

func containsIgnoreCase(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	// Simple case-insensitive contains
	ls := toLower(s)
	lsub := toLower(substr)
	return len(ls) >= len(lsub) && contains(ls, lsub)
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
