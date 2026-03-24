package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Metadata keys — must match the BFF client interceptor keys exactly.
const (
	MDKeyUserID         = "x-user-id"
	MDKeyUserEmail      = "x-user-email"
	MDKeyUserProvider   = "x-user-provider"
	MDKeyProviderUserID = "x-provider-user-id"
)

// ── context keys ──

type (
	userIDKey         struct{}
	userEmailKey      struct{}
	userProviderKey   struct{}
	providerUserIDKey struct{}
)

// ── context helpers ──

// UserIDFromContext returns the user ID injected by the server interceptor.
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userIDKey{}).(string)
	return v
}

// UserEmailFromContext returns the user email injected by the server interceptor.
func UserEmailFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userEmailKey{}).(string)
	return v
}

// UserProviderFromContext returns the auth provider injected by the server interceptor.
func UserProviderFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userProviderKey{}).(string)
	return v
}

// ProviderUserIDFromContext returns the provider-specific user ID injected by the server interceptor.
func ProviderUserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(providerUserIDKey{}).(string)
	return v
}

// ── interceptors ──

// UnaryServerInterceptor extracts user metadata from incoming gRPC metadata
// and injects it into the Go context so downstream handlers / services can
// access it via the *FromContext helpers.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		ctx = extractUserMetadata(ctx)
		return handler(ctx, req)
	}
}

// StreamServerInterceptor does the same for streaming RPCs.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := extractUserMetadata(ss.Context())
		wrapped := newWrappedServerStream(ss, ctx)
		return handler(srv, wrapped)
	}
}

// extractUserMetadata reads the x-user-* keys from incoming gRPC metadata
// and stores them in the context.
func extractUserMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	if userID := firstValue(md, MDKeyUserID); userID != "" {
		ctx = context.WithValue(ctx, userIDKey{}, userID)
	}
	if email := firstValue(md, MDKeyUserEmail); email != "" {
		ctx = context.WithValue(ctx, userEmailKey{}, email)
	}
	if provider := firstValue(md, MDKeyUserProvider); provider != "" {
		ctx = context.WithValue(ctx, userProviderKey{}, provider)
	}
	if providerUID := firstValue(md, MDKeyProviderUserID); providerUID != "" {
		ctx = context.WithValue(ctx, providerUserIDKey{}, providerUID)
	}

	return ctx
}

func firstValue(md metadata.MD, key string) string {
	vals := md.Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// wrappedServerStream wraps grpc.ServerStream to allow overriding Context().
//
// The grpc.ServerStream interface requires a Context() method with no parameters,
// so context cannot be passed as a method argument — it must be stored in the struct.
// This is the standard gRPC Go pattern for enriching the stream context in interceptors.
// The stored context is immutable after construction and scoped to a single RPC lifetime.
type wrappedServerStream struct {
	grpc.ServerStream
	streamCtx context.Context //nolint:containedctx // required: grpc.ServerStream.Context() takes no params, context must live in struct
}

// newWrappedServerStream constructs a wrappedServerStream with the enriched context.
func newWrappedServerStream(ss grpc.ServerStream, ctx context.Context) *wrappedServerStream {
	return &wrappedServerStream{ServerStream: ss, streamCtx: ctx}
}

// Context returns the enriched context stored at construction time.
func (w *wrappedServerStream) Context() context.Context {
	return w.streamCtx
}