package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"refina-transaction/config/log"
	"refina-transaction/interface/grpc/interceptor"
	"refina-transaction/internal/repository"
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

	pageSize := req.GetPageSize()

	q := repository.CursorQuery{
		WalletIDs:    req.GetWalletIds(),
		WalletID:     req.GetWalletId(),
		CategoryID:   req.GetCategoryId(),
		CategoryType: req.GetCategoryType(),
		DateFrom:     req.GetDateFrom(),
		DateTo:       req.GetDateTo(),
		Search:       req.GetSearch(),
		SortBy:       req.GetSortBy(),
		SortOrder:    req.GetSortOrder(),
		PageSize:     int(pageSize),
		Cursor:       req.GetCursor(),
		CursorAmount: req.GetCursorAmount(),
		CursorDate:   req.GetCursorDate(),
	}

	results, total, err := s.transactionService.GetTransactionsByCursor(ctx, q)
	if err != nil {
		log.Error(data.LogGetUserTransactionsFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("get user transactions: %w", err)
	}

	hasNext := false
	if pageSize > 0 && int32(len(results)) > pageSize {
		hasNext = true
		results = results[:pageSize]
	}

	protoTxns := make([]*tpb.TransactionDetail, 0, len(results))
	for _, txn := range results {
		protoTxns = append(protoTxns, toProtoTransactionDetail(txn))
	}

	resp := &tpb.GetUserTransactionsResponse{
		Transactions: protoTxns,
		Total:        int32(total),
		PageSize:     pageSize,
		HasNext:      hasNext,
	}

	if len(results) > 0 {
		last := results[len(results)-1]
		resp.NextCursor = last.ID
		resp.NextCursorAmount = last.Amount
		resp.NextCursorDate = last.TransactionDate.Format(time.RFC3339)
	}

	log.Info(data.LogGetUserTransactionsSuccess, map[string]any{
		"service":   data.GRPCServerService,
		"user_id":   userID,
		"total":     total,
		"page_size": pageSize,
		"has_next":  hasNext,
	})

	return resp, nil
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
		IsWalletNotCreated: req.GetIsWalletNotCreated(),
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

// buildCategoryGroupsFromView converts view categories to proto category groups.
// Extracted to reduce cognitive complexity (go:S3776).
func buildCategoryGroupsFromView(viewCategories []dto.CategoriesResponse) []*tpb.CategoryGroup {
	groups := make([]*tpb.CategoryGroup, 0, len(viewCategories))
	for _, cg := range viewCategories {
		items := make([]*tpb.CategoryItem, 0, len(cg.Category))
		for _, c := range cg.Category {
			items = append(items, &tpb.CategoryItem{
				Id:   c.ID,
				Name: c.Name,
			})
		}
		groups = append(groups, &tpb.CategoryGroup{
			GroupName:  cg.GroupName,
			Type:       string(cg.Type),
			Categories: items,
		})
	}
	return groups
}

func (s *transactionServer) GetCategories(ctx context.Context, req *tpb.GetCategoriesRequest) (*tpb.GetCategoriesResponse, error) {
	userID := interceptor.UserIDFromContext(ctx)
	filterType := req.GetType()

	categoryGroups, err := s.fetchCategoryGroups(ctx, filterType)
	if err != nil {
		log.Error(data.LogGetCategoriesGRPCFailed, map[string]any{
			"service": data.GRPCServerService,
			"user_id": userID,
			"type":    filterType,
			"error":   err.Error(),
		})
		return nil, err
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

// fetchCategoryGroups retrieves category groups, filtering by type when provided.
// Extracted to reduce cognitive complexity (go:S3776).
func (s *transactionServer) fetchCategoryGroups(ctx context.Context, filterType string) ([]*tpb.CategoryGroup, error) {
	if filterType != "" {
		viewCategories, err := s.categoryService.GetCategoriesByType(ctx, filterType)
		if err != nil {
			return nil, fmt.Errorf("get categories by type [type=%s]: %w", filterType, err)
		}

		groups := make([]*tpb.CategoryGroup, 0, len(viewCategories))
		for _, vg := range viewCategories {
			items := make([]*tpb.CategoryItem, 0, len(vg.Category))
			for _, c := range vg.Category {
				items = append(items, &tpb.CategoryItem{
					Id:   c.ID,
					Name: c.Name,
				})
			}
			groups = append(groups, &tpb.CategoryGroup{
				GroupName:  vg.GroupName,
				Type:       vg.Type,
				Categories: items,
			})
		}
		return groups, nil
	}

	categories, err := s.categoryService.GetAllCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("get all categories: %w", err)
	}
	return buildCategoryGroupsFromView(categories), nil
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
// Admin Master Data RPCs
// ──────────────────────────────────────────────────────────────────────────────

// filterCategories applies type and search filters to a flat category list.
// Extracted to reduce cognitive complexity (go:S3776).
func filterCategories(allCategories []dto.CategoryFlat, filterType string, search string) []dto.CategoryFlat {
	var filtered []dto.CategoryFlat
	for _, c := range allCategories {
		if filterType != "" && string(c.Type) != filterType {
			continue
		}
		if search != "" {
			s := strings.ToLower(search)
			if !strings.Contains(strings.ToLower(c.Name), s) && !strings.Contains(strings.ToLower(string(c.Type)), s) {
				continue
			}
		}
		filtered = append(filtered, c)
	}
	return filtered
}

// sortCategories sorts a flat category list by the given field and order.
// Extracted to reduce cognitive complexity (go:S3776).
func sortCategories(filtered []dto.CategoryFlat, sortBy string, sortOrder string) {
	sort.Slice(filtered, func(i, j int) bool {
		var cmp bool
		switch sortBy {
		case "name":
			cmp = strings.ToLower(filtered[i].Name) < strings.ToLower(filtered[j].Name)
		case "type":
			cmp = string(filtered[i].Type) < string(filtered[j].Type)
		default:
			cmp = filtered[i].ID < filtered[j].ID
		}
		if sortOrder == "desc" {
			return !cmp
		}
		return cmp
	})
}

func (s *transactionServer) ListCategories(ctx context.Context, req *tpb.ListCategoriesRequest) (*tpb.ListCategoriesResponse, error) {
	page := int(req.GetPage())
	if page < 1 {
		page = 1
	}
	pageSize := int(req.GetPageSize())
	if pageSize < 1 {
		pageSize = 10
	}
	sortBy := req.GetSortBy()
	if sortBy == "" {
		sortBy = "created_at"
	}
	sortOrder := req.GetSortOrder()
	if sortOrder == "" {
		sortOrder = "desc"
	}

	allCategories, err := s.categoryService.GetAllCategoriesFlat(ctx)
	if err != nil {
		log.Error(data.LogListCategoriesFailed, map[string]any{
			"service": data.GRPCServerService,
			"error":   err.Error(),
		})
		return nil, fmt.Errorf("list categories: %w", err)
	}

	filtered := filterCategories(allCategories, req.GetType(), req.GetSearch())

	total := int32(len(filtered))
	totalPages := total / int32(pageSize)
	if total%int32(pageSize) != 0 {
		totalPages++
	}

	sortCategories(filtered, sortBy, sortOrder)

	start := (page - 1) * pageSize
	end := start + pageSize
	if start > len(filtered) {
		start = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}
	paged := filtered[start:end]

	var protoCategories []*tpb.CategoryDetail
	for _, c := range paged {
		protoCategories = append(protoCategories, &tpb.CategoryDetail{
			Id:         c.ID,
			ParentId:   c.ParentID,
			ParentName: c.ParentName,
			Name:       c.Name,
			Type:       string(c.Type),
			CreatedAt:  c.CreatedAt,
			UpdatedAt:  c.UpdatedAt,
		})
	}

	log.Info(data.LogListCategoriesSuccess, map[string]any{
		"service": data.GRPCServerService,
		"total":   total,
		"page":    page,
	})

	return &tpb.ListCategoriesResponse{
		Categories: protoCategories,
		Total:      total,
		Page:       int32(page),
		PageSize:   int32(pageSize),
		TotalPages: totalPages,
	}, nil
}

func (s *transactionServer) GetCategoryDetail(ctx context.Context, req *tpb.CategoryID) (*tpb.CategoryDetail, error) {
	id := req.GetId()

	category, err := s.categoryService.GetCategoryByID(ctx, id)
	if err != nil {
		log.Error(data.LogGetCategoryDetailFailed, map[string]any{
			"service":     data.GRPCServerService,
			"category_id": id,
			"error":       err.Error(),
		})
		return nil, fmt.Errorf("get category detail [id=%s]: %w", id, err)
	}

	parentID := ""
	parentName := ""
	categoryID := ""
	categoryName := ""
	categoryType := ""

	if len(category.Category) > 0 {
		categoryID = category.Category[0].ID
		categoryName = category.Category[0].Name
	} else {
		categoryName = category.GroupName
	}

	if category.GroupName != "" && len(category.Category) > 0 {
		parentName = category.GroupName
	}

	categoryType = string(category.Type)

	if categoryID == "" {
		categoryID = id
	}

	log.Info(data.LogGetCategoryDetailSuccess, map[string]any{
		"service":     data.GRPCServerService,
		"category_id": id,
	})

	return &tpb.CategoryDetail{
		Id:         categoryID,
		ParentId:   parentID,
		ParentName: parentName,
		Name:       categoryName,
		Type:       categoryType,
	}, nil
}