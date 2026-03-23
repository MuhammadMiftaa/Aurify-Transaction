package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"refina-transaction/config/db"
	"refina-transaction/config/env"
	logger "refina-transaction/config/log"
	"refina-transaction/config/miniofs"
	"refina-transaction/interface/grpc/client"
	grpcserver "refina-transaction/interface/grpc/server"
	"refina-transaction/interface/http/router"
	queueclient "refina-transaction/interface/queue/client"
	"refina-transaction/interface/queue/setup"
	"refina-transaction/internal/repository"
	"refina-transaction/internal/service"
	"refina-transaction/internal/utils"
	"refina-transaction/internal/utils/data"
)

func init() {
	var err error
	var missing []string
	if missing, err = env.LoadByViper(); err != nil {
		log.Printf("Failed to read JSON config file: %v", err)
		if missing, err = env.LoadNative(); err != nil {
			log.Fatalf("Failed to load environment variables: %v", err)
		}
		log.Printf("Environment variables by .env file loaded successfully")
	} else {
		log.Printf("Environment variables by Viper loaded successfully")
	}

	logger.SetupLogger()

	if len(missing) > 0 {
		for _, envVar := range missing {
			logger.Warn(data.LogEnvVarMissing, map[string]any{"service": data.EnvService, "env_var": envVar})
		}
	}
}

// shutdownHTTP gracefully shuts down the HTTP server and records errors.
// Extracted to reduce cognitive complexity (go:S3776).
func shutdownHTTP(ctx context.Context, httpServer *http.Server, errors map[string]any) {
	if httpServer == nil {
		return
	}
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error(data.LogHTTPServerShutdownFailed, map[string]any{"service": data.HTTPServerService, "error": err.Error()})
		errors["http_error"] = true
	}
}

// shutdownGRPC gracefully stops the gRPC server and closes its client connections.
// Extracted to reduce cognitive complexity (go:S3776).
func shutdownGRPC(ctx context.Context, grpcServer interface{ GracefulStop() }, grpcManager *client.GRPCClientManager, errors map[string]any) {
	if grpcServer == nil {
		return
	}
	grpcServer.GracefulStop()
	if err := grpcManager.Shutdown(ctx); err != nil {
		logger.Error(data.LogGRPCClientShutdownFailed, map[string]any{"service": data.GRPCClientService, "error": err.Error()})
		errors["grpc_error"] = true
	}
}

// shutdownInfra closes RabbitMQ and database connections.
// Extracted to reduce cognitive complexity (go:S3776).
func shutdownInfra(queueInstance queueclient.RabbitMQClient, dbInstance db.DatabaseClient, errors map[string]any) {
	if err := queueInstance.Close(); err != nil {
		logger.Error(data.LogRabbitmqCloseFailed, map[string]any{"service": data.RabbitmqService, "error": err.Error()})
		errors["rabbitmq_error"] = true
	}

	if err := dbInstance.Close(); err != nil {
		logger.Error(data.LogDBCloseFailed, map[string]any{"service": data.DatabaseService, "error": err.Error()})
		errors["database_error"] = true
	}
}

func main() {
	// Setup Database Connection
	startTime := time.Now()
	dbInstance := db.GetInstance(env.Cfg.Database)
	logger.Info(data.LogDBSetupSuccess, map[string]any{"service": data.DatabaseService, "duration": utils.Ms(time.Since(startTime))})

	// Setup MinIO Connection
	startTime = time.Now()
	minioInstance := miniofs.SetupMinio(env.Cfg.Minio)
	logger.Info(data.LogMinioSetupSuccess, map[string]any{"service": data.MinioService, "duration": utils.Ms(time.Since(startTime))})

	// Setup RabbitMQ Connection
	startTime = time.Now()
	queueInstance := queueclient.GetInstance(env.Cfg.RabbitMQ)
	logger.Info(data.LogRabbitmqSetupSuccess, map[string]any{"service": data.RabbitmqService, "duration": utils.Ms(time.Since(startTime))})

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup Outbox Publisher
	startTime = time.Now()
	outboxRepo := repository.NewOutboxRepository(dbInstance.GetDB())
	outboxPublisher := service.NewOutboxPublisher(outboxRepo, queueInstance)

	go outboxPublisher.Start(ctx)
	go outboxPublisher.StartCleanupJob(ctx)
	logger.Info(data.LogOutboxPublisherStarted, map[string]any{"service": data.OutboxService, "duration": utils.Ms(time.Since(startTime))})

	// Set up the gRPC client
	startTime = time.Now()
	grpcManager := client.GetManager()
	err := grpcManager.SetupGRPCClient()
	if err != nil {
		logger.Fatal(data.LogGRPCClientSetupFailed, map[string]any{"service": data.GRPCClientService, "error": err.Error()})
	}
	logger.Info(data.LogGRPCClientSetupSuccess, map[string]any{"service": data.GRPCClientService, "duration": utils.Ms(time.Since(startTime))})

	// Setup Queue Consumers
	startTime = time.Now()
	setup.SetupQueueConsumers(ctx, dbInstance, minioInstance, queueInstance)
	logger.Info(data.LogInvestmentConsumerStarted, map[string]any{"service": data.InvestmentConsumerService, "duration": utils.Ms(time.Since(startTime))})

	// Set up the HTTP server
	startTime = time.Now()
	httpServer := router.SetupHTTPServer(dbInstance, minioInstance, queueInstance)
	if httpServer != nil {
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Fatal(data.LogHTTPServerStartFailed, map[string]any{"service": data.HTTPServerService, "error": err.Error()})
			}
		}()
		logger.Info(data.LogHTTPServerStarted, map[string]any{"service": data.HTTPServerService, "port": env.Cfg.Server.HTTPPort, "duration": utils.Ms(time.Since(startTime))})
	}

	// Set up the gRPC server
	startTime = time.Now()
	grpcServer, lis, err := grpcserver.SetupGRPCServer(dbInstance, minioInstance, queueInstance)
	if err != nil {
		logger.Fatal(data.LogGRPCServerSetupFailed, map[string]any{"service": data.GRPCServerService, "error": err.Error()})
	}
	if grpcServer != nil && lis != nil {
		go func() {
			if err := grpcServer.Serve(*lis); err != nil {
				logger.Fatal(data.LogGRPCServerServeFailed, map[string]any{"service": data.GRPCServerService, "error": err.Error()})
			}
		}()
		logger.Info(data.LogGRPCServerStarted, map[string]any{"service": data.GRPCServerService, "port": env.Cfg.Server.GRPCPort, "duration": utils.Ms(time.Since(startTime))})
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info(data.LogShutdownSignalReceived, map[string]any{"service": data.MainService})

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	startTime = time.Now()
	shutdownErrors := map[string]any{"service": data.MainService}

	shutdownHTTP(shutdownCtx, httpServer, shutdownErrors)

	// Cancel context to stop outbox publisher
	cancel()
	time.Sleep(2 * time.Second)

	shutdownGRPC(shutdownCtx, grpcServer, grpcManager, shutdownErrors)
	shutdownInfra(queueInstance, dbInstance, shutdownErrors)

	if len(shutdownErrors) > 1 {
		logger.Info(data.LogShutdownCompletedWithErrors, shutdownErrors)
	} else {
		logger.Info(data.LogShutdownCompleted, map[string]any{"service": data.MainService, "duration": utils.Ms(time.Since(startTime))})
	}
}