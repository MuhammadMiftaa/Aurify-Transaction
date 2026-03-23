package env

import (
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
const errEnvNotSet = " env is not set"

// lookupEnv reads an OS environment variable; if missing it appends a message to missing.
func lookupEnv(key string, dest *string, missing *[]string) {
	if val, ok := os.LookupEnv(key); ok {
		*dest = val
	} else {
		*missing = append(*missing, key+errEnvNotSet)
	}
}

// lookupEnvInt reads an integer env var; appends error message on missing or invalid value.
func lookupEnvInt(key string, dest *int, missing *[]string) {
	val, ok := os.LookupEnv(key)
	if !ok {
		*missing = append(*missing, key+errEnvNotSet)
		return
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		*missing = append(*missing, key+" must be integer, got "+val)
		return
	}
	*dest = n
}

// LoadNative loads configuration from OS environment variables (or a .env file).
func LoadNative() ([]string, error) {
	if _, err := os.Stat("/app/.env"); err == nil {
		if err := godotenv.Load(); err != nil {
			return nil, err
		}
	}

	var missing []string

	lookupEnv("MODE", &Cfg.Server.Mode, &missing)
	lookupEnv("HTTP_PORT", &Cfg.Server.HTTPPort, &missing)
	lookupEnv("GRPC_PORT", &Cfg.Server.GRPCPort, &missing)

	lookupEnv("DB_HOST", &Cfg.Database.DBHost, &missing)
	lookupEnv("DB_PORT", &Cfg.Database.DBPort, &missing)
	lookupEnv("DB_USER", &Cfg.Database.DBUser, &missing)
	lookupEnv("DB_PASSWORD", &Cfg.Database.DBPassword, &missing)
	lookupEnv("DB_NAME", &Cfg.Database.DBName, &missing)

	lookupEnv("MINIO_HOST", &Cfg.Minio.Host, &missing)
	lookupEnv("MINIO_ROOT_USER", &Cfg.Minio.AccessKey, &missing)
	lookupEnv("MINIO_ROOT_PASSWORD", &Cfg.Minio.SecretKey, &missing)
	lookupEnvInt("MINIO_MAX_OPEN_CONN", &Cfg.Minio.MaxOpenConn, &missing)
	lookupEnvInt("MINIO_USE_SSL", &Cfg.Minio.UseSSL, &missing)
	lookupEnv("MINIO_PUBLIC_URL", &Cfg.Minio.PublicURL, &missing)

	lookupEnv("WALLET_ADDRESS", &Cfg.GRPCConfig.WalletAddress, &missing)

	lookupEnv("RABBITMQ_HOST", &Cfg.RabbitMQ.RMQHost, &missing)
	lookupEnv("RABBITMQ_PORT", &Cfg.RabbitMQ.RMQPort, &missing)
	lookupEnv("RABBITMQ_USER", &Cfg.RabbitMQ.RMQUser, &missing)
	lookupEnv("RABBITMQ_PASSWORD", &Cfg.RabbitMQ.RMQPassword, &missing)
	lookupEnv("RABBITMQ_VIRTUAL_HOST", &Cfg.RabbitMQ.RMQVirtualHost, &missing)

	return missing, nil
}

// viperGet reads a viper key as string; if empty appends message to missing.
func viperGet(v *viper.Viper, key string, dest *string, missing *[]string) {
	if val := v.GetString(key); val != "" {
		*dest = val
	} else {
		*missing = append(*missing, key+errEnvNotSet)
	}
}

// viperGetInt reads a viper key as int; appends message if not set or invalid.
func viperGetInt(v *viper.Viper, key string, dest *int, missing *[]string) {
	val := v.GetInt(key)
	if val != 0 {
		*dest = val
	} else {
		*missing = append(*missing, key+" env is not set or invalid")
	}
}

// LoadByViper loads configuration from a JSON config file via Viper.
func LoadByViper() ([]string, error) {
	config := viper.New()
	if configFile, err := os.Stat("/app/config.json"); err != nil || configFile.IsDir() {
		config.SetConfigFile("config.json")
	} else {
		config.SetConfigFile("/app/config.json")
	}

	if err := config.ReadInConfig(); err != nil {
		return nil, err
	}

	var missing []string

	viperGet(config, "MODE", &Cfg.Server.Mode, &missing)
	viperGet(config, "HTTP_PORT", &Cfg.Server.HTTPPort, &missing)
	viperGet(config, "GRPC_PORT", &Cfg.Server.GRPCPort, &missing)

	viperGet(config, "DATABASE.POSTGRESQL.HOST", &Cfg.Database.DBHost, &missing)
	viperGet(config, "DATABASE.POSTGRESQL.PORT", &Cfg.Database.DBPort, &missing)
	viperGet(config, "DATABASE.POSTGRESQL.USER", &Cfg.Database.DBUser, &missing)
	viperGet(config, "DATABASE.POSTGRESQL.PASSWORD", &Cfg.Database.DBPassword, &missing)
	viperGet(config, "DATABASE.POSTGRESQL.NAME", &Cfg.Database.DBName, &missing)

	viperGet(config, "OBJECT-STORAGE.MINIO.HOST", &Cfg.Minio.Host, &missing)
	viperGet(config, "OBJECT-STORAGE.MINIO.USER", &Cfg.Minio.AccessKey, &missing)
	viperGet(config, "OBJECT-STORAGE.MINIO.PASSWORD", &Cfg.Minio.SecretKey, &missing)
	viperGetInt(config, "OBJECT-STORAGE.MINIO.MAX_OPEN_CONN_POOL", &Cfg.Minio.MaxOpenConn, &missing)

	useSSL := config.GetInt("OBJECT-STORAGE.MINIO.USE_SSL")
	if useSSL == 0 && config.IsSet("OBJECT-STORAGE.MINIO.USE_SSL") == false {
		missing = append(missing, "OBJECT-STORAGE.MINIO.USE_SSL env is not set")
	} else if useSSL != 0 && useSSL != 1 {
		missing = append(missing, "OBJECT-STORAGE.MINIO.USE_SSL must be 0 or 1")
	}
	Cfg.Minio.UseSSL = useSSL

	viperGet(config, "OBJECT-STORAGE.MINIO.PUBLIC_URL", &Cfg.Minio.PublicURL, &missing)

	viperGet(config, "GRPC-CONFIG.WALLET_ADDRESS", &Cfg.GRPCConfig.WalletAddress, &missing)

	viperGet(config, "MESSAGE-BROKER.RABBITMQ.HOST", &Cfg.RabbitMQ.RMQHost, &missing)
	viperGet(config, "MESSAGE-BROKER.RABBITMQ.PORT", &Cfg.RabbitMQ.RMQPort, &missing)
	viperGet(config, "MESSAGE-BROKER.RABBITMQ.USER", &Cfg.RabbitMQ.RMQUser, &missing)
	viperGet(config, "MESSAGE-BROKER.RABBITMQ.PASSWORD", &Cfg.RabbitMQ.RMQPassword, &missing)
	viperGet(config, "MESSAGE-BROKER.RABBITMQ.VIRTUAL_HOST", &Cfg.RabbitMQ.RMQVirtualHost, &missing)

	return missing, nil
}