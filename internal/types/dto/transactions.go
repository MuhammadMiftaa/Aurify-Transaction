package dto

import "time"

type TransactionsResponse struct {
	ID string `json:"id"`

	WalletID     string `json:"wallet_id"`
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name"`
	CategoryType string `json:"category_type"`

	Amount          float64   `json:"amount"`
	TransactionDate time.Time `json:"transaction_date"`
	Description     string    `json:"description"`

	Attachments []AttachmentsResponse `json:"attachments"`
}

type UpdateAttachmentsRequest struct {
	Status string   `json:"status"`
	Files  []string `json:"files"`
}

type TransactionsRequest struct {
	WalletID    string                     `json:"wallet_id"`
	CategoryID  string                     `json:"category_id"`
	Amount      float64                    `json:"amount"`
	Date        time.Time                  `json:"date"`
	Description string                     `json:"description"`
	Attachments []UpdateAttachmentsRequest `json:"attachments"`

	// Indicates if the wallet was created during the transaction use event
	IsWalletNotCreated bool
}

type FundTransferResponse struct {
	CashInTransactionID  string    `json:"cash_in_transaction_id"`
	CashOutTransactionID string    `json:"cash_out_transaction_id"`
	FromWalletID         string    `json:"from_wallet_id"`
	ToWalletID           string    `json:"to_wallet_id"`
	Amount               float64   `json:"amount"`
	Date                 time.Time `json:"date"`
	Description          string    `json:"description"`
}

type FundTransferRequest struct {
	CashInCategoryID  string    `json:"cash_in_category_id"`
	CashOutCategoryID string    `json:"cash_out_category_id"`
	FromWalletID      string    `json:"from_wallet_id"`
	ToWalletID        string    `json:"to_wallet_id"`
	Amount            float64   `json:"amount"`
	AdminFee          float64   `json:"admin_fee"`
	Date              time.Time `json:"date"`
	Description       string    `json:"description"`
}
