package service

import (
	"context"

	"refina-transaction/internal/repository"
	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/types/model"
	"refina-transaction/internal/types/view"

	"github.com/google/uuid"
)

type CategoriesService interface {
	GetAllCategories(ctx context.Context) ([]dto.CategoriesResponse, error)
	GetAllCategoriesFlat(ctx context.Context) ([]dto.CategoryFlat, error)
	GetCategoryByID(ctx context.Context, id string) (dto.CategoriesResponse, error)
	GetCategoriesByType(ctx context.Context, typeCategory string) ([]view.ViewCategoriesGroupByType, error)
	CreateCategory(ctx context.Context, category dto.CategoriesRequest) (dto.CategoriesResponse, error)
	UpdateCategory(ctx context.Context, id string, category dto.CategoriesRequest) (dto.CategoriesResponse, error)
	DeleteCategory(ctx context.Context, id string) (dto.CategoriesResponse, error)
}

type categoriesService struct {
	txManager          repository.TxManager
	categoryRepository repository.CategoriesRepository
}

func NewCategoriesService(txManager repository.TxManager, categoryRepository repository.CategoriesRepository) CategoriesService {
	return &categoriesService{
		txManager:          txManager,
		categoryRepository: categoryRepository,
	}
}

func (category_serv *categoriesService) GetAllCategories(ctx context.Context) ([]dto.CategoriesResponse, error) {
	categories, err := category_serv.categoryRepository.GetAllCategories(ctx, nil)
	if err != nil {
		return nil, err
	}

	var groupedCategories []dto.CategoriesResponse
	for _, category := range categories {
		if category.ParentID == nil {
			exists := false
			for _, group := range groupedCategories {
				if group.GroupName == category.Name {
					exists = true
					break
				}
			}
			if !exists {
				groupName := category.Name
				groupedCategories = append(groupedCategories, dto.CategoriesResponse{
					GroupName: groupName,
					Type:      dto.CategoryType(category.Type),
					Category:  []dto.Category{},
				})
			}
		} else {
			if category.Parent != nil {
				for i, group := range groupedCategories {
					if group.GroupName == category.Parent.Name {
						groupedCategories[i].Category = append(groupedCategories[i].Category, dto.Category{
							ID:   category.ID.String(),
							Name: category.Name,
						})
					}
				}
			}
		}
	}

	return groupedCategories, nil
}

func (category_serv *categoriesService) GetAllCategoriesFlat(ctx context.Context) ([]dto.CategoryFlat, error) {
	categories, err := category_serv.categoryRepository.GetAllCategories(ctx, nil)
	if err != nil {
		return nil, err
	}

	var flat []dto.CategoryFlat
	for _, c := range categories {
		parentID := ""
		parentName := ""
		if c.ParentID != nil {
			parentID = c.ParentID.String()
		}
		if c.Parent != nil {
			parentName = c.Parent.Name
		}

		flat = append(flat, dto.CategoryFlat{
			ID:         c.ID.String(),
			ParentID:   parentID,
			ParentName: parentName,
			Name:       c.Name,
			Type:       dto.CategoryType(c.Type),
			CreatedAt:  c.CreatedAt.Format("2006-01-02T15:04:05Z"),
			UpdatedAt:  c.UpdatedAt.Format("2006-01-02T15:04:05Z"),
		})
	}

	return flat, nil
}

func (category_serv *categoriesService) GetCategoryByID(ctx context.Context, id string) (dto.CategoriesResponse, error) {
	category, err := category_serv.categoryRepository.GetCategoryByID(ctx, nil, id)
	if err != nil {
		return dto.CategoriesResponse{}, err
	}

	var response dto.CategoriesResponse

	if category.Parent != nil {
		var categories []dto.Category
		categories = append(categories, dto.Category{
			ID:   category.ID.String(),
			Name: category.Name,
		})
		response = dto.CategoriesResponse{
			GroupName: category.Parent.Name,
			Type:      dto.CategoryType(category.Type),
			Category:  categories,
		}
	} else {
		response = dto.CategoriesResponse{
			GroupName: category.Name,
		}
	}

	return response, nil
}

func (category_serv *categoriesService) GetCategoriesByType(ctx context.Context, typeCategory string) ([]view.ViewCategoriesGroupByType, error) {
	categories, err := category_serv.categoryRepository.GetCategoriesByType(ctx, nil, typeCategory)
	if err != nil {
		return nil, err
	}

	return categories, nil
}

func (category_serv *categoriesService) CreateCategory(ctx context.Context, category dto.CategoriesRequest) (dto.CategoriesResponse, error) {
	var newCategory model.Categories
	var parentName string
	var err error
	if category.ParentID != "" {
		parent, err := category_serv.categoryRepository.GetCategoryByID(ctx, nil, category.ParentID)
		if err != nil {
			return dto.CategoriesResponse{}, err
		}

		parentName = parent.Name

		newCategory, err = category_serv.categoryRepository.CreateCategory(ctx, nil, model.Categories{
			ParentID: &parent.ID,
			Name:     category.Name,
			Type:     parent.Type,
		})
		if err != nil {
			return dto.CategoriesResponse{}, err
		}
	} else {
		newCategory, err = category_serv.categoryRepository.CreateCategory(ctx, nil, model.Categories{
			Name: category.Name,
			Type: model.CategoryType(category.Type),
		})
		if err != nil {
			return dto.CategoriesResponse{}, err
		}
	}

	var response dto.CategoriesResponse
	if category.ParentID != "" {
		var categories []dto.Category
		categories = append(categories, dto.Category{
			ID:   newCategory.ID.String(),
			Name: newCategory.Name,
		})
		response = dto.CategoriesResponse{
			GroupName: parentName,
			Type:      dto.CategoryType(newCategory.Type),
			Category:  categories,
		}
	} else {
		response = dto.CategoriesResponse{
			GroupName: newCategory.Name,
			Type:      dto.CategoryType(newCategory.Type),
		}
	}

	return response, nil
}

func (category_serv *categoriesService) UpdateCategory(ctx context.Context, id string, category dto.CategoriesRequest) (dto.CategoriesResponse, error) {
	existCategory, err := category_serv.categoryRepository.GetCategoryByID(ctx, nil, id)
	if err != nil {
		return dto.CategoriesResponse{}, err
	}

	if category.ParentID != "" {
		_, err := category_serv.categoryRepository.GetCategoryByID(ctx, nil, category.ParentID)
		if err != nil {
			return dto.CategoriesResponse{}, err
		}

		parentUUID, err := uuid.Parse(category.ParentID)
		if err != nil {
			return dto.CategoriesResponse{}, err
		}
		existCategory.ParentID = &parentUUID
	}
	if category.Name != "" {
		existCategory.Name = category.Name
	}
	if category.Type != "" && category.ParentID == "" {
		existCategory.Type = model.CategoryType(category.Type)
	}

	newCategory, err := category_serv.categoryRepository.UpdateCategory(ctx, nil, existCategory)
	if err != nil {
		return dto.CategoriesResponse{}, err
	}

	var response dto.CategoriesResponse
	if existCategory.ParentID != nil {
		var categories []dto.Category
		categories = append(categories, dto.Category{
			ID:   newCategory.ID.String(),
			Name: newCategory.Name,
		})
		response = dto.CategoriesResponse{
			GroupName: existCategory.Parent.Name,
			Type:      dto.CategoryType(newCategory.Type),
			Category:  categories,
		}
	} else {
		response = dto.CategoriesResponse{
			GroupName: newCategory.Name,
		}
	}

	return response, nil
}

func (category_serv *categoriesService) DeleteCategory(ctx context.Context, id string) (dto.CategoriesResponse, error) {
	existCategory, err := category_serv.categoryRepository.GetCategoryByID(ctx, nil, id)
	if err != nil {
		return dto.CategoriesResponse{}, err
	}

	deletedCategory, err := category_serv.categoryRepository.DeleteCategory(ctx, nil, existCategory)
	if err != nil {
		return dto.CategoriesResponse{}, err
	}

	var response dto.CategoriesResponse
	if existCategory.ParentID != nil {
		var categories []dto.Category
		categories = append(categories, dto.Category{
			ID:   deletedCategory.ID.String(),
			Name: deletedCategory.Name,
		})
		response = dto.CategoriesResponse{
			GroupName: existCategory.Parent.Name,
			Type:      dto.CategoryType(deletedCategory.Type),
			Category:  categories,
		}
	} else {
		response = dto.CategoriesResponse{
			GroupName: deletedCategory.Name,
		}
	}
	return response, nil
}
