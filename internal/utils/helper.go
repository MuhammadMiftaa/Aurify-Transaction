package utils

import (
	"time"

	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/types/model"

	"github.com/google/uuid"
)

func ConvertToResponseType(data any) any {
	switch v := data.(type) {
	case model.Attachments:
		return dto.AttachmentsResponse{
			ID:            v.ID.String(),
			TransactionID: v.TransactionID.String(),
			Image:         v.Image,
			Format:        v.Format,
			Size:          v.Size,
		}
	case []model.Attachments:
		if len(v) == 0 {
			return []dto.AttachmentsResponse{}
		}
		responses := make([]dto.AttachmentsResponse, len(v))
		for i, att := range v {
			responses[i] = ConvertToResponseType(att).(dto.AttachmentsResponse)
		}
		return responses
	case model.Transactions:
		return dto.TransactionsResponse{
			ID:              v.ID.String(),
			WalletID:        v.WalletID.String(),
			CategoryID:      v.CategoryID.String(),
			CategoryName:    v.Category.Name,
			CategoryType:    string(v.Category.Type),
			Amount:          v.Amount,
			TransactionDate: v.TransactionDate,
			Description:     v.Description,
			Attachments:     ConvertToResponseType(v.Attachments).([]dto.AttachmentsResponse),
		}
	default:
		return nil
	}
}

func ParseUUID(id string) (uuid.UUID, error) {
	parsedID, err := uuid.Parse(id)
	if err != nil {
		return uuid.UUID{}, err
	}
	return parsedID, nil
}

func Ms(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

func SameDate(t1, t2 time.Time) bool {
	y1, m1, d1 := t1.Date()
	y2, m2, d2 := t2.Date()
	return y1 == y2 && m1 == m2 && d1 == d2
}