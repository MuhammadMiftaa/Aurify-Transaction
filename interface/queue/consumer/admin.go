package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"refina-transaction/config/log"
	"refina-transaction/interface/queue/client"
	"refina-transaction/internal/service"
	"refina-transaction/internal/types/dto"
	"refina-transaction/internal/utils/data"

	"github.com/rabbitmq/amqp091-go"
)

type AdminEventConsumer struct {
	rabbitMQ        client.RabbitMQClient
	categoryService service.CategoriesService
}

func NewAdminEventConsumer(rmq client.RabbitMQClient, catService service.CategoriesService) *AdminEventConsumer {
	return &AdminEventConsumer{
		rabbitMQ:        rmq,
		categoryService: catService,
	}
}

func (c *AdminEventConsumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info(data.LogAdminConsumerStopped, map[string]any{"service": data.AdminConsumerService})
			return
		default:
			if err := c.consume(ctx); err != nil {
				log.Error(data.LogAdminConsumerFailed, map[string]any{
					"service": data.AdminConsumerService,
					"error":   err.Error(),
				})
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}
}

func (c *AdminEventConsumer) consume(ctx context.Context) error {
	channel, err := c.rabbitMQ.GetChannel()
	if err != nil {
		return fmt.Errorf("get channel: %w", err)
	}
	defer channel.Close()

	// Declare admin exchange
	if err := channel.ExchangeDeclare(data.ADMIN_EXCHANGE, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	q, err := channel.QueueDeclare(data.ADMIN_QUEUE, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if err := channel.QueueBind(q.Name, data.ADMIN_ROUTING_KEY, data.ADMIN_EXCHANGE, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	msgs, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	log.Info(data.LogAdminConsumerStarted, map[string]any{"service": data.AdminConsumerService})

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("channel closed")
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				log.Error(data.LogAdminEventHandleFailed, map[string]any{
					"service": data.AdminConsumerService,
					"error":   err.Error(),
				})
				_ = msg.Nack(false, true)
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

type adminEvent struct {
	Action      string                 `json:"action"`
	AggregateID string                 `json:"aggregate_id"`
	Data        map[string]interface{} `json:"data"`
	Timestamp   string                 `json:"timestamp"`
}

func (c *AdminEventConsumer) handleMessage(ctx context.Context, msg amqp091.Delivery) error {
	var event adminEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("unmarshal admin event: %w", err)
	}

	switch event.Action {
	case "create":
		return c.handleCreate(ctx, event)
	case "update":
		return c.handleUpdate(ctx, event)
	case "delete":
		return c.handleDelete(ctx, event)
	default:
		log.Warn(data.LogAdminEventUnknown, map[string]any{
			"service": data.AdminConsumerService,
			"action":  event.Action,
		})
		return nil
	}
}

func (c *AdminEventConsumer) handleCreate(ctx context.Context, event adminEvent) error {
	req := dto.CategoriesRequest{
		Name:     getString(event.Data, "name"),
		Type:     dto.CategoryType(getString(event.Data, "type")),
		ParentID: getString(event.Data, "parent_id"),
	}

	_, err := c.categoryService.CreateCategory(ctx, req)
	if err != nil {
		return fmt.Errorf("create category: %w", err)
	}

	log.Info(data.LogAdminCategoryCreated, map[string]any{
		"service": data.AdminConsumerService,
		"name":    req.Name,
	})

	return nil
}

func (c *AdminEventConsumer) handleUpdate(ctx context.Context, event adminEvent) error {
	id := event.AggregateID
	req := dto.CategoriesRequest{
		Name:     getString(event.Data, "name"),
		Type:     dto.CategoryType(getString(event.Data, "type")),
		ParentID: getString(event.Data, "parent_id"),
	}

	_, err := c.categoryService.UpdateCategory(ctx, id, req)
	if err != nil {
		return fmt.Errorf("update category [id=%s]: %w", id, err)
	}

	log.Info(data.LogAdminCategoryUpdated, map[string]any{
		"service":     data.AdminConsumerService,
		"category_id": id,
	})

	return nil
}

func (c *AdminEventConsumer) handleDelete(ctx context.Context, event adminEvent) error {
	id := event.AggregateID

	_, err := c.categoryService.DeleteCategory(ctx, id)
	if err != nil {
		return fmt.Errorf("delete category [id=%s]: %w", id, err)
	}

	log.Info(data.LogAdminCategoryDeleted, map[string]any{
		"service":     data.AdminConsumerService,
		"category_id": id,
	})

	return nil
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}
