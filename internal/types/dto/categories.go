package dto

type CategoryType string

const (
	Income       CategoryType = "income"
	Expense      CategoryType = "expense"
	FundTransfer CategoryType = "fund_transfer"
)

type Category struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type CategoriesResponse struct {
	GroupName string       `json:"group_name"`
	Category  []Category   `json:"category"`
	Type      CategoryType `json:"type"`
}

type CategoriesRequest struct {
	ParentID string       `json:"parent_id"`
	Name     string       `json:"name"`
	Type     CategoryType `json:"type"`
}

// CategoryFlat is a flat representation of a category for admin listing.
type CategoryFlat struct {
	ID         string       `json:"id"`
	ParentID   string       `json:"parent_id"`
	ParentName string       `json:"parent_name"`
	Name       string       `json:"name"`
	Type       CategoryType `json:"type"`
	CreatedAt  string       `json:"created_at"`
	UpdatedAt  string       `json:"updated_at"`
}
