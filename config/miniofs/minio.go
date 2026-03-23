package miniofs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"refina-transaction/config/env"
	"refina-transaction/config/log"
	constant "refina-transaction/internal/utils/data"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// String constants to avoid duplication (go:S1192)
const (
	TRANSACTION_ATTACHMENT_BUCKET = "refina-transaction-attachments"
	TRANSACTION_ATTACHMENT_PREFIX = "transaction_attachments"
	errMinIOClientNotReady        = "MinIO client not ready"
	contentTypeOctetStream        = "application/octet-stream"
	contentTypeGif                = "image/gif"
	contentTypeZip                = "application/zip"
)

// FileValidationConfig holds validation rules
type FileValidationConfig struct {
	AllowedExtensions []string
	MaxFileSize       int64 // in bytes
	MinFileSize       int64 // in bytes
}

// UploadRequest represents file upload request
type UploadRequest struct {
	Base64Data string
	Prefix     string // prefix for filename, will be combined with timestamp and extension
	BucketName string
	Validation *FileValidationConfig
}

// UploadResponse represents upload result
type UploadResponse struct {
	BucketName string
	ObjectName string
	Size       int64
	URL        string
	Ext        string
	ETag       string
}

type MinIOConfig struct {
	Host           string
	AccessKey      string
	SecretKey      string
	UseSSL         bool
	MaxConnections int
	ConnectTimeout time.Duration
	RequestTimeout time.Duration
}

// Global MinIO manager - singleton pattern seperti database/redis
type MinIOManager struct {
	client      *minio.Client
	config      MinIOConfig
	mu          sync.RWMutex
	isReady     bool
	bucketCache map[string]bool // cache untuk bucket existence check
}

var (
	MinioClient *MinIOManager
	once        sync.Once
)

// Init initializes global MinIO manager - dipanggil sekali di main.go
func SetupMinio(cfg env.Minio) *MinIOManager {
	once.Do(func() {
		cfg := MinIOConfig{
			Host:           cfg.Host,
			AccessKey:      cfg.AccessKey,
			SecretKey:      cfg.SecretKey,
			UseSSL:         cfg.UseSSL == 1,
			ConnectTimeout: 30 * time.Second,
			RequestTimeout: 60 * time.Second,
		}

		var err error
		MinioClient, err = newMinIOManager(cfg)
		if err != nil {
			log.Log.Fatalf("Failed to initialize MinIO: %v", err)
		}
	})

	return MinioClient
}

// newMinIOManager creates new MinIO manager
func newMinIOManager(cfg MinIOConfig) (*MinIOManager, error) {
	client, err := minio.New(cfg.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %v", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	// Simple health check by listing buckets
	_, err = client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MinIO: %v", err)
	}

	return &MinIOManager{
		client:      client,
		config:      cfg,
		isReady:     true,
		bucketCache: make(map[string]bool),
	}, nil
}

// Health check method
func (m *MinIOManager) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isReady
}

// validateBucketName checks basic naming rules for a bucket.
// Extracted to reduce cognitive complexity (go:S3776).
func validateBucketName(bucketName string) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}
	if strings.Contains(bucketName, " ") || strings.Contains(bucketName, "_") {
		return fmt.Errorf("bucket name cannot contain spaces or underscores")
	}
	return nil
}

// validateBucket checks if bucket exists with caching
func (m *MinIOManager) validateBucket(ctx context.Context, bucketName string) error {
	if err := validateBucketName(bucketName); err != nil {
		return err
	}

	// Check cache first
	m.mu.RLock()
	if exists, found := m.bucketCache[bucketName]; found {
		m.mu.RUnlock()
		if !exists {
			return fmt.Errorf("bucket '%s' not found", bucketName)
		}
		return nil
	}
	m.mu.RUnlock()

	// Check bucket existence
	exists, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %v", err)
	}

	// Update cache
	m.mu.Lock()
	m.bucketCache[bucketName] = exists
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("bucket '%s' not found", bucketName)
	}

	return nil
}

// DecodeFile decodes base64 string to byte array and extracts content type
func (m *MinIOManager) DecodeFile(base64Data string) ([]byte, string, error) {
	if base64Data == "" {
		return nil, "", fmt.Errorf("base64 data cannot be empty")
	}

	var contentType string
	// Extract content type from data URL if exists
	if strings.HasPrefix(base64Data, "data:") {
		if idx := strings.Index(base64Data, ";base64,"); idx != -1 {
			contentType = base64Data[5:idx]
			base64Data = base64Data[idx+8:]
		}
	}

	// Decode base64
	decoded, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode base64: %v", err)
	}

	// If content type not found, detect from content
	if contentType == "" {
		contentType = getContentTypeFromData(decoded)
	}

	return decoded, contentType, nil
}

// Validate validates file based on provided rules
func (m *MinIOManager) Validate(data []byte, contentType string, config *FileValidationConfig) error {
	fileSize := int64(len(data))

	if config.MaxFileSize > 0 && fileSize > config.MaxFileSize {
		return fmt.Errorf("file size (%d bytes) exceeds maximum allowed size (%d bytes)",
			fileSize, config.MaxFileSize)
	}
	if config.MinFileSize > 0 && fileSize < config.MinFileSize {
		return fmt.Errorf("file size (%d bytes) is below minimum required size (%d bytes)",
			fileSize, config.MinFileSize)
	}

	if err := m.validateExtension(contentType, config.AllowedExtensions); err != nil {
		return err
	}

	// Content consistency check
	if len(data) > 0 {
		detectedType := getContentTypeFromData(data)
		if detectedType != contentTypeOctetStream && contentType != detectedType {
			log.Warn("content_type_mismatch", map[string]any{
				"service":       constant.MinioService,
				"declared_type": contentType,
				"detected_type": detectedType,
			})
		}
	}

	return nil
}

// validateExtension checks if the content type maps to an allowed extension.
// Extracted to reduce cognitive complexity (go:S3776).
func (m *MinIOManager) validateExtension(contentType string, allowedExtensions []string) error {
	if len(allowedExtensions) == 0 {
		return nil
	}

	ext := getExtensionFromContentType(contentType)
	if ext == "" {
		return fmt.Errorf("unable to determine file extension from content type: %s", contentType)
	}

	for _, allowedExt := range allowedExtensions {
		if ext == strings.ToLower(allowedExt) {
			return nil
		}
	}
	return fmt.Errorf("file type '%s' (extension '%s') is not allowed. Allowed extensions: %v",
		contentType, ext, allowedExtensions)
}

// UploadFile uploads file to MinIO - main method yang digunakan
func (m *MinIOManager) UploadFile(ctx context.Context, request UploadRequest) (*UploadResponse, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf(errMinIOClientNotReady)
	}

	// Validate bucket
	if err := m.validateBucket(ctx, request.BucketName); err != nil {
		return nil, err
	}

	// Decode and validate file
	data, contentType, err := m.DecodeFile(request.Base64Data)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}

	if request.Validation == nil {
		request.Validation = CreateDefaultValidationConfig()
	}

	if err := m.Validate(data, contentType, request.Validation); err != nil {
		return nil, fmt.Errorf("validation error: %v", err)
	}

	// Generate object name
	ext := getExtensionFromContentType(contentType)
	if ext == "" {
		ext = ".bin"
	}

	prefix := request.Prefix
	if prefix == "" {
		prefix = "file"
	}

	timestamp := time.Now().Unix()
	objectName := fmt.Sprintf("%s_%d%s", prefix, timestamp, ext)

	// Upload file
	reader := bytes.NewReader(data)
	options := minio.PutObjectOptions{
		ContentType: contentType,
	}

	info, err := m.client.PutObject(ctx, request.BucketName, objectName, reader, int64(len(data)), options)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %v", err)
	}

	// Generate URL
	fileURL := fmt.Sprintf("%s://%s/%s/%s",
		getProtocol(m.config.UseSSL),
		m.config.Host,
		request.BucketName,
		objectName)

	return &UploadResponse{
		BucketName: request.BucketName,
		ObjectName: objectName,
		Size:       info.Size,
		URL:        fileURL,
		Ext:        ext,
		ETag:       info.ETag,
	}, nil
}

// GetFile retrieves file from MinIO
func (m *MinIOManager) GetFile(ctx context.Context, bucketName, objectName string) (*minio.Object, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf(errMinIOClientNotReady)
	}

	if err := m.validateBucket(ctx, bucketName); err != nil {
		return nil, err
	}

	return m.client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
}

// DeleteFile deletes file from MinIO
func (m *MinIOManager) DeleteFile(ctx context.Context, bucketName, objectName string) error {
	if !m.IsReady() {
		return fmt.Errorf(errMinIOClientNotReady)
	}

	if err := m.validateBucket(ctx, bucketName); err != nil {
		return err
	}

	return m.client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
}

// GetPresignedURL generates presigned URL for direct access
func (m *MinIOManager) GetPresignedURL(ctx context.Context, bucketName, objectName string, expires time.Duration) (string, error) {
	if !m.IsReady() {
		return "", fmt.Errorf(errMinIOClientNotReady)
	}

	if err := m.validateBucket(ctx, bucketName); err != nil {
		return "", err
	}

	presignedURL, err := m.client.PresignedGetObject(ctx, bucketName, objectName, expires, nil)
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %v", err)
	}

	return presignedURL.String(), nil
}

// ListObjects lists objects in bucket with prefix
func (m *MinIOManager) ListObjects(ctx context.Context, bucketName, prefix string) ([]minio.ObjectInfo, error) {
	if !m.IsReady() {
		return nil, fmt.Errorf(errMinIOClientNotReady)
	}

	if err := m.validateBucket(ctx, bucketName); err != nil {
		return nil, err
	}

	var objects []minio.ObjectInfo
	objectCh := m.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		objects = append(objects, object)
	}

	return objects, nil
}

// Helper functions
func getContentTypeFromData(data []byte) string {
	if len(data) == 0 {
		return contentTypeOctetStream
	}

	signatures := map[string]string{
		"\xFF\xD8\xFF":      "image/jpeg",
		"\x89PNG\r\n\x1A\n": "image/png",
		"GIF87a":            contentTypeGif,
		"GIF89a":            contentTypeGif,
		"\x00\x00\x01\x00":  "image/x-icon",
		"RIFF":              "image/webp",
		"%PDF":              "application/pdf",
		"PK\x03\x04":        contentTypeZip,
		"PK\x05\x06":        contentTypeZip,
		"PK\x07\x08":        contentTypeZip,
	}

	dataStr := string(data[:minInt(len(data), 10)])
	for signature, ct := range signatures {
		if strings.HasPrefix(dataStr, signature) {
			return ct
		}
	}
	return contentTypeOctetStream
}

func getExtensionFromContentType(contentType string) string {
	extensions := map[string]string{
		"image/jpeg":               ".jpg",
		"image/jpg":                ".jpg",
		"image/png":                ".png",
		contentTypeGif:             ".gif",
		"image/webp":               ".webp",
		"image/x-icon":             ".ico",
		"image/vnd.microsoft.icon": ".ico",
		"application/pdf":          ".pdf",
		contentTypeZip:             ".zip",
		"application/json":         ".json",
		"text/plain":               ".txt",
		"text/html":                ".html",
		"text/css":                 ".css",
		"text/javascript":          ".js",
		"application/javascript":   ".js",
		"video/mp4":                ".mp4",
		"video/webm":               ".webm",
		"audio/mp3":                ".mp3",
		"audio/mpeg":               ".mp3",
		"audio/wav":                ".wav",
		"application/msword":       ".doc",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
		"application/vnd.ms-excel": ".xls",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
	}

	if ext, exists := extensions[contentType]; exists {
		return ext
	}
	if parts := strings.Split(contentType, "/"); len(parts) == 2 {
		return "." + parts[1]
	}
	return ""
}

func getProtocol(useSSL bool) string {
	if useSSL {
		return "https"
	}
	return "http"
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func CreateDefaultValidationConfig() *FileValidationConfig {
	return &FileValidationConfig{
		AllowedExtensions: []string{".jpg", ".jpeg", ".png", ".gif", ".pdf", ".doc", ".docx"},
		MaxFileSize:       10 * 1024 * 1024, // 10MB
		MinFileSize:       1,                // 1 byte
	}
}

func CreateImageValidationConfig() *FileValidationConfig {
	return &FileValidationConfig{
		AllowedExtensions: []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf"},
		MaxFileSize:       5 * 1024 * 1024, // 5MB
		MinFileSize:       1,               // 1 byte
	}
}

// ParseMinioURL menerima URL MinIO/S3 dan mengembalikan bucket + objectKey
func ParseMinioURL(rawURL string) (bucket, objectKey string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", err
	}

	// path selalu dimulai dengan "/", jadi hapus dulu
	path := strings.TrimPrefix(u.Path, "/")

	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("URL is not valid: %s", rawURL)
	}

	bucket = parts[0]
	objectKey = parts[1]
	return bucket, objectKey, nil
}

func ExtractObjectNameFromURL(rawURL string) string {
	parts := strings.Split(rawURL, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func ReplaceURL(rawURL string) string {
	separator := "/" + TRANSACTION_ATTACHMENT_BUCKET
	parts := strings.Split(rawURL, separator)
	if len(parts) < 2 {
		return rawURL
	}
	parts[0] = env.Cfg.Minio.PublicURL
	return strings.Join(parts, separator)
}
