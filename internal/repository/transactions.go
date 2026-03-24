package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"refina-transaction/internal/types/model"

	"gorm.io/gorm"
)

// CursorQuery holds cursor-based pagination and filter parameters.
type CursorQuery struct {
	WalletIDs    []string
	WalletID     string // single wallet filter
	CategoryID   string
	CategoryType string
	DateFrom     string
	DateTo       string
	Search       string
	SortBy       string // "transaction_date" or "amount"
	SortOrder    string // "asc" or "desc"
	PageSize     int
	Cursor       string  // last item ID from previous page
	CursorAmount float64 // cursor amount value (when sorting by amount)
	CursorDate   string  // cursor date value (when sorting by date)
}

type TransactionsRepository interface {
	GetAllTransactions(ctx context.Context, tx Transaction) ([]model.Transactions, error)
	GetTransactionByID(ctx context.Context, tx Transaction, id string) (model.Transactions, error)
	GetTransactionsByWalletIDs(ctx context.Context, tx Transaction, ids []string) ([]model.Transactions, error)
	GetTransactionsByCursor(ctx context.Context, tx Transaction, q CursorQuery) ([]model.Transactions, int64, error)
	CreateTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error)
	UpdateTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error)
	DeleteTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error)
}

type transactionsRepository struct {
	db *gorm.DB
}

func NewTransactionRepository(db *gorm.DB) TransactionsRepository {
	return &transactionsRepository{db}
}

func (transaction_repo *transactionsRepository) getDB(ctx context.Context, tx Transaction) (*gorm.DB, error) {
	if tx != nil {
		gormTx, ok := tx.(*GormTx)
		if !ok {
			return nil, errors.New("invalid transaction type")
		}
		return gormTx.db.WithContext(ctx), nil
	}
	return transaction_repo.db.WithContext(ctx), nil
}

func (transaction_repo *transactionsRepository) GetAllTransactions(ctx context.Context, tx Transaction) ([]model.Transactions, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return nil, err
	}

	var transactions []model.Transactions
	if err := db.Joins("Category").Order("transaction_date DESC").Find(&transactions).Error; err != nil {
		return nil, errors.New("user transactions not found")
	}
	return transactions, nil
}

func (trasaction_repo *transactionsRepository) GetTransactionByID(ctx context.Context, tx Transaction, id string) (model.Transactions, error) {
	db, err := trasaction_repo.getDB(ctx, tx)
	if err != nil {
		return model.Transactions{}, err
	}

	var transaction model.Transactions
	if err := db.Joins("Category").Where("\"transactions\".id = ?", id).First(&transaction).Error; err != nil {
		return model.Transactions{}, errors.New("transaction not found")
	}

	return transaction, nil
}

func (transaction_repo *transactionsRepository) GetTransactionsByWalletIDs(ctx context.Context, tx Transaction, ids []string) ([]model.Transactions, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return nil, err
	}

	var transactions []model.Transactions
	if err := db.Joins("Category").Preload("Attachments").Where("\"transactions\".wallet_id IN ?", ids).Order("transaction_date DESC").Find(&transactions).Error; err != nil {
		return nil, errors.New("user transactions not found")
	}
	return transactions, nil
}

func (transaction_repo *transactionsRepository) CreateTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return model.Transactions{}, err
	}

	if err := db.Omit("Category", "Attachments").Create(&transaction).Error; err != nil {
		return model.Transactions{}, err
	}

	return transaction, nil
}

func (transaction_repo *transactionsRepository) UpdateTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return model.Transactions{}, err
	}

	if err := db.Omit("Wallet", "Category").Save(&transaction).Error; err != nil {
		return model.Transactions{}, err
	}

	return transaction, nil
}

func (transaction_repo *transactionsRepository) DeleteTransaction(ctx context.Context, tx Transaction, transaction model.Transactions) (model.Transactions, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return model.Transactions{}, err
	}

	if err := db.Delete(&transaction).Error; err != nil {
		return model.Transactions{}, err
	}
	return transaction, nil
}

// applyBaseFilters applies wallet, category, date, and search filters to the query.
// Extracted to reduce cognitive complexity (go:S3776).
func applyBaseFilters(base *gorm.DB, q CursorQuery) *gorm.DB {
	if q.WalletID != "" {
		base = base.Where("transactions.wallet_id = ?", q.WalletID)
	}
	if q.CategoryID != "" {
		base = base.Where("transactions.category_id = ?", q.CategoryID)
	}
	if q.CategoryType != "" {
		base = base.Joins("JOIN categories AS cat_filter ON cat_filter.id = transactions.category_id AND cat_filter.deleted_at IS NULL").
			Where("cat_filter.type = ?", q.CategoryType)
	}
	if q.DateFrom != "" {
		if t, err := time.Parse(time.RFC3339, q.DateFrom); err == nil {
			base = base.Where("transactions.transaction_date >= ?", t)
		}
	}
	if q.DateTo != "" {
		if t, err := time.Parse(time.RFC3339, q.DateTo); err == nil {
			base = base.Where("transactions.transaction_date <= ?", t)
		}
	}
	if q.Search != "" {
		like := "%" + strings.ToLower(q.Search) + "%"
		base = base.Where("LOWER(transactions.description) LIKE ?", like)
	}
	return base
}

// applyCursorCondition applies keyset pagination cursor condition to the query.
// Extracted to reduce cognitive complexity (go:S3776).
func applyCursorCondition(base *gorm.DB, q CursorQuery, sortBy, sortOrder string) *gorm.DB {
	if q.Cursor == "" {
		return base
	}

	op := "<"
	if sortOrder == "asc" {
		op = ">"
	}

	if sortBy == "amount" {
		return base.Where(
			fmt.Sprintf("(transactions.amount %s ?) OR (transactions.amount = ? AND transactions.id %s ?)", op, op),
			q.CursorAmount, q.CursorAmount, q.Cursor,
		)
	}

	// default: transaction_date
	if q.CursorDate != "" {
		if cursorTime, err := time.Parse(time.RFC3339, q.CursorDate); err == nil {
			return base.Where(
				fmt.Sprintf("(transactions.transaction_date %s ?) OR (transactions.transaction_date = ? AND transactions.id %s ?)", op, op),
				cursorTime, cursorTime, q.Cursor,
			)
		}
	}
	return base
}

func (transaction_repo *transactionsRepository) GetTransactionsByCursor(ctx context.Context, tx Transaction, q CursorQuery) ([]model.Transactions, int64, error) {
	db, err := transaction_repo.getDB(ctx, tx)
	if err != nil {
		return nil, 0, err
	}

	// ── Base query: wallet IDs ──
	base := db.Model(&model.Transactions{}).Where("transactions.wallet_id IN ?", q.WalletIDs)

	// ── Apply filters ──
	base = applyBaseFilters(base, q)

	// ── Count total (before cursor) ──
	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, errors.New("failed to count transactions")
	}

	// ── Sort config ──
	sortBy := q.SortBy
	if sortBy == "" {
		sortBy = "transaction_date"
	}
	sortOrder := strings.ToLower(q.SortOrder)
	if sortOrder != "asc" {
		sortOrder = "desc"
	}

	// ── Cursor condition (keyset) ──
	base = applyCursorCondition(base, q, sortBy, sortOrder)

	// ── Order + limit ──
	pageSize := q.PageSize
	if pageSize <= 0 {
		pageSize = 9999
	}

	orderClause := fmt.Sprintf("transactions.%s %s, transactions.id %s", sortBy, sortOrder, sortOrder)

	var transactions []model.Transactions
	if err := base.Joins("Category").Preload("Attachments").
		Order(orderClause).
		Limit(pageSize + 1). // fetch one extra to determine has_next
		Find(&transactions).Error; err != nil {
		return nil, 0, errors.New("failed to fetch transactions")
	}

	return transactions, total, nil
}