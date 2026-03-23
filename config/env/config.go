package env

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type (
	Server struct {
		Mode     string `env:"MODE"`
		HTTPPort string `env:"HTTP_PORT"`
		GRPCPort string `env:"GRPC_PORT"`
	}

	Database struct {
		DBHost     string `env:"DB_HOST"`
		DBPort     string `env:"DB_PORT"`
		DBUser     string `env:"DB_USER"`
		DBPassword string `env:"DB_PASSWORD"`
		DBName     string `env:"DB_NAME"`
	}

	Minio struct {
		Host        string `env:"MINIO_HOST"`
		AccessKey   string `env:"MINIO_ROOT_USER"`
		SecretKey   string `env:"MINIO_ROOT_PASSWORD"`
		MaxOpenConn int    `env:"MINIO_MAX_OPEN_CONN"`
		UseSSL      int    `env:"MINIO_USE_SSL"`
		PublicURL   string `env:"MINIO_PUBLIC_URL"`
	}

	GRPCConfig struct {
		WalletAddress string `env:"WALLET_ADDRESS"`
	}

	RabbitMQ struct {
		RMQHost        string `env:"RABBITMQ_HOST"`
		RMQPort        string `env:"RABBITMQ_PORT"`
		RMQUser        string `env:"RABBITMQ_USER"`
		RMQPassword    string `env:"RABBITMQ_PASSWORD"`
		RMQVirtualHost string `env:"RABBITMQ_VIRTUAL_HOST"`
	}

	Config struct {
		Server     Server
		Database   Database
		Minio      Minio
		GRPCConfig GRPCConfig
		RabbitMQ   RabbitMQ
	}
)

var Cfg Config

func LoadNative() ([]string, error) {
	var ok bool
	var missing []string

	if _, err := os.Stat("/app/.env"); err == nil {
		if err := godotenv.Load(); err != nil {
			return nil, err
		}
	}

	// ! Load Server configuration ____________________________
	if Cfg.Server.Mode, ok = os.LookupEnv("MODE"); !ok {
		missing = append(missing, "MODE env is not set")
	}
	if Cfg.Server.HTTPPort, ok = os.LookupEnv("HTTP_PORT"); !ok {
		missing = append(missing, "HTTP_PORT env is not set")
	}
	if Cfg.Server.GRPCPort, ok = os.LookupEnv("GRPC_PORT"); !ok {
		missing = append(missing, "GRPC_PORT env is not set")
	}
	// ! ______________________________________________________

	// ! Load Database configuration __________________________
	if Cfg.Database.DBUser, ok = os.LookupEnv("DB_USER"); !ok {
		missing = append(missing, "DB_USER env is not set")
	}
	if Cfg.Database.DBHost, ok = os.LookupEnv("DB_HOST"); !ok {
		missing = append(missing, "DB_HOST env is not set")
	}
	if Cfg.Database.DBPort, ok = os.LookupEnv("DB_PORT"); !ok {
		missing = append(missing, "DB_PORT env is not set")
	}
	if Cfg.Database.DBName, ok = os.LookupEnv("DB_NAME"); !ok {
		missing = append(missing, "DB_NAME env is not set")
	}
	if Cfg.Database.DBPassword, ok = os.LookupEnv("DB_PASSWORD"); !ok {
		missing = append(missing, "DB_PASSWORD env is not set")
	}
	// ! ______________________________________________________

	// ! Load MinIO configuration _____________________________
	if Cfg.Minio.Host, ok = os.LookupEnv("MINIO_HOST"); !ok {
		missing = append(missing, "MINIO_HOST env is not set")
	}
	if Cfg.Minio.AccessKey, ok = os.LookupEnv("MINIO_ROOT_USER"); !ok {
		missing = append(missing, "MINIO_ROOT_USER env is not set")
	}
	if Cfg.Minio.SecretKey, ok = os.LookupEnv("MINIO_ROOT_PASSWORD"); !ok {
		missing = append(missing, "MINIO_ROOT_PASSWORD env is not set")
	}
	if val, ok := os.LookupEnv("MINIO_MAX_OPEN_CONN"); !ok {
		missing = append(missing, "MINIO_MAX_OPEN_CONN env is not set")
	} else {
		var err error
		if Cfg.Minio.MaxOpenConn, err = strconv.Atoi(val); err != nil {
			missing = append(missing, fmt.Sprintf("MINIO_MAX_OPEN_CONN must be int, got %s", val))
		}
	}
	if val, ok := os.LookupEnv("MINIO_USE_SSL"); !ok {
		missing = append(missing, "MINIO_USE_SSL env is not set")
	} else {
		var err error
		if Cfg.Minio.UseSSL, err = strconv.Atoi(val); err != nil {
			missing = append(missing, fmt.Sprintf("MINIO_USE_SSL must be int, got %s", val))
		}
	}
	if val, ok := os.LookupEnv("MINIO_PUBLIC_URL"); !ok {
		missing = append(missing, "MINIO_PUBLIC_URL env is not set")
	} else {
		Cfg.Minio.PublicURL = val
	}
	// ! ______________________________________________________

	// ! Load gRPC configuration _____________________________
	if Cfg.GRPCConfig.WalletAddress, ok = os.LookupEnv("WALLET_ADDRESS"); !ok {
		missing = append(missing, "WALLET_ADDRESS env is not set")
	}
	// ! ______________________________________________________

	// ! Load RabbitMQ configuration __________________________
	if Cfg.RabbitMQ.RMQUser, ok = os.LookupEnv("RABBITMQ_USER"); !ok {
		missing = append(missing, "RABBITMQ_USER env is not set")
	}
	if Cfg.RabbitMQ.RMQPassword, ok = os.LookupEnv("RABBITMQ_PASSWORD"); !ok {
		missing = append(missing, "RABBITMQ_PASSWORD env is not set")
	}
	if Cfg.RabbitMQ.RMQHost, ok = os.LookupEnv("RABBITMQ_HOST"); !ok {
		missing = append(missing, "RABBITMQ_HOST env is not set")
	}
	if Cfg.RabbitMQ.RMQPort, ok = os.LookupEnv("RABBITMQ_PORT"); !ok {
		missing = append(missing, "RABBITMQ_PORT env is not set")
	}
	if Cfg.RabbitMQ.RMQVirtualHost, ok = os.LookupEnv("RABBITMQ_VIRTUAL_HOST"); !ok {
		missing = append(missing, "RABBITMQ_VIRTUAL_HOST env is not set")
	}
	// ! ______________________________________________________

	return missing, nil
}

func LoadByViper() ([]string, error) {
	var missing []string

	config := viper.New()
	if configFile, err := os.Stat("/app/config.json"); err != nil || configFile.IsDir() {
		config.SetConfigFile("config.json")
	} else {
		config.SetConfigFile("/app/config.json")
	}

	if err := config.ReadInConfig(); err != nil {
		return nil, err
	}

	// ! Load Server configuration ____________________________
	if Cfg.Server.Mode = config.GetString("MODE"); Cfg.Server.Mode == "" {
		missing = append(missing, "MODE env is not set")
	}
	if Cfg.Server.HTTPPort = config.GetString("HTTP_PORT"); Cfg.Server.HTTPPort == "" {
		missing = append(missing, "HTTP_PORT env is not set")
	}
	if Cfg.Server.GRPCPort = config.GetString("GRPC_PORT"); Cfg.Server.GRPCPort == "" {
		missing = append(missing, "GRPC_PORT env is not set")
	}
	// ! ______________________________________________________

	// ! Load Database configuration __________________________
	if Cfg.Database.DBUser = config.GetString("DATABASE.POSTGRESQL.USER"); Cfg.Database.DBUser == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.USER env is not set")
	}
	if Cfg.Database.DBHost = config.GetString("DATABASE.POSTGRESQL.HOST"); Cfg.Database.DBHost == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.HOST env is not set")
	}
	if Cfg.Database.DBPort = config.GetString("DATABASE.POSTGRESQL.PORT"); Cfg.Database.DBPort == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.PORT env is not set")
	}
	if Cfg.Database.DBName = config.GetString("DATABASE.POSTGRESQL.NAME"); Cfg.Database.DBName == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.NAME env is not set")
	}
	if Cfg.Database.DBPassword = config.GetString("DATABASE.POSTGRESQL.PASSWORD"); Cfg.Database.DBPassword == "" {
		missing = append(missing, "DATABASE.POSTGRESQL.PASSWORD env is not set")
	}
	// ! ______________________________________________________

	// ! Load Minio configuration __________________________
	if Cfg.Minio.Host = config.GetString("OBJECT-STORAGE.MINIO.HOST"); Cfg.Minio.Host == "" {
		missing = append(missing, "OBJECT-STORAGE.MINIO.HOST env is not set")
	}
	if Cfg.Minio.AccessKey = config.GetString("OBJECT-STORAGE.MINIO.USER"); Cfg.Minio.AccessKey == "" {
		missing = append(missing, "OBJECT-STORAGE.MINIO.USER env is not set")
	}
	if Cfg.Minio.SecretKey = config.GetString("OBJECT-STORAGE.MINIO.PASSWORD"); Cfg.Minio.SecretKey == "" {
		missing = append(missing, "OBJECT-STORAGE.MINIO.PASSWORD env is not set")
	}
	if Cfg.Minio.MaxOpenConn = config.GetInt("OBJECT-STORAGE.MINIO.MAX_OPEN_CONN_POOL"); Cfg.Minio.MaxOpenConn == 0 {
		missing = append(missing, "OBJECT-STORAGE.MINIO.MAX_OPEN_CONN_POOL env is not set")
	}
	if Cfg.Minio.UseSSL = config.GetInt("OBJECT-STORAGE.MINIO.USE_SSL"); Cfg.Minio.UseSSL < 0 || Cfg.Minio.UseSSL > 1 {
		missing = append(missing, "OBJECT-STORAGE.MINIO.USE_SSL env is not valid")
	}
	if Cfg.Minio.PublicURL = config.GetString("OBJECT-STORAGE.MINIO.PUBLIC_URL"); Cfg.Minio.PublicURL == "" {
		missing = append(missing, "OBJECT-STORAGE.MINIO.PUBLIC_URL env is not set")
	}
	// ! ______________________________________________________

	// ! Load gRPC configuration _____________________________
	if Cfg.GRPCConfig.WalletAddress = config.GetString("GRPC-CONFIG.WALLET_ADDRESS"); Cfg.GRPCConfig.WalletAddress == "" {
		missing = append(missing, "GRPC-CONFIG.WALLET_ADDRESS env is not set")
	}
	// ! ______________________________________________________

	// ! Load RabbitMQ configuration __________________________
	if Cfg.RabbitMQ.RMQUser = config.GetString("MESSAGE-BROKER.RABBITMQ.USER"); Cfg.RabbitMQ.RMQUser == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.USER env is not set")
	}
	if Cfg.RabbitMQ.RMQPassword = config.GetString("MESSAGE-BROKER.RABBITMQ.PASSWORD"); Cfg.RabbitMQ.RMQPassword == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.PASSWORD env is not set")
	}
	if Cfg.RabbitMQ.RMQHost = config.GetString("MESSAGE-BROKER.RABBITMQ.HOST"); Cfg.RabbitMQ.RMQHost == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.HOST env is not set")
	}
	if Cfg.RabbitMQ.RMQPort = config.GetString("MESSAGE-BROKER.RABBITMQ.PORT"); Cfg.RabbitMQ.RMQPort == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.PORT env is not set")
	}
	if Cfg.RabbitMQ.RMQVirtualHost = config.GetString("MESSAGE-BROKER.RABBITMQ.VIRTUAL_HOST"); Cfg.RabbitMQ.RMQVirtualHost == "" {
		missing = append(missing, "MESSAGE-BROKER.RABBITMQ.VIRTUAL_HOST env is not set")
	}
	// ! ______________________________________________________

	return missing, nil
}
