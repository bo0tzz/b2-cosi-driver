package b2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/Backblaze/blazer/b2"
	"github.com/Backblaze/blazer/base"
)

// ErrBucketNotEmpty is returned when attempting to delete a non-empty bucket.
var ErrBucketNotEmpty = errors.New("bucket is not empty")

// Client wraps the blazer B2 SDK for COSI driver operations.
type Client struct {
	b2 *b2.Client
}

// NewClient creates a new B2 client authenticating with an application key.
func NewClient(ctx context.Context, keyID, keySecret string) (*Client, error) {
	b2c, err := b2.NewClient(ctx, keyID, keySecret)
	if err != nil {
		return nil, fmt.Errorf("authorizing B2 account: %w", err)
	}
	return &Client{b2: b2c}, nil
}

// GetBucketByName returns S3 endpoint and region for the named bucket,
// or found=false if no such bucket exists in the account.
func (c *Client) GetBucketByName(ctx context.Context, name string) (s3Endpoint, region string, found bool, err error) {
	bucket, err := c.b2.Bucket(ctx, name)
	if err != nil {
		if b2.IsNotExist(err) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("looking up bucket %q: %w", name, err)
	}
	s3Endpoint = bucket.S3URL()
	region = extractRegion(s3Endpoint)
	return s3Endpoint, region, true, nil
}

// CreateBucket creates a B2 bucket (idempotent: returns existing bucket info if already present).
func (c *Client) CreateBucket(ctx context.Context, name string, public bool) (s3Endpoint, region string, err error) {
	var bType b2.BucketType = b2.Private
	if public {
		bType = b2.Public
	}
	bucket, err := c.b2.NewBucket(ctx, name, &b2.BucketAttrs{Type: bType})
	if err != nil {
		return "", "", fmt.Errorf("creating bucket %q: %w", name, err)
	}
	s3Endpoint = bucket.S3URL()
	region = extractRegion(s3Endpoint)
	return s3Endpoint, region, nil
}

// DeleteBucket deletes the named bucket. Returns nil if not found (idempotent).
// Returns ErrBucketNotEmpty if the bucket has objects.
func (c *Client) DeleteBucket(ctx context.Context, bucketName string) error {
	bucket, err := c.b2.Bucket(ctx, bucketName)
	if err != nil {
		if b2.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("looking up bucket %q: %w", bucketName, err)
	}
	if err := bucket.Delete(ctx); err != nil {
		if b2.IsNotExist(err) {
			return nil
		}
		if isBucketNotEmpty(err) {
			return ErrBucketNotEmpty
		}
		return fmt.Errorf("deleting bucket %q: %w", bucketName, err)
	}
	return nil
}

// CreateApplicationKey creates a scoped application key for the named bucket.
// Capabilities are restricted to read-only or read-write based on readOnly.
func (c *Client) CreateApplicationKey(ctx context.Context, name, bucketName string, readOnly bool) (keyID, keySecret string, err error) {
	bucket, err := c.b2.Bucket(ctx, bucketName)
	if err != nil {
		return "", "", fmt.Errorf("looking up bucket %q: %w", bucketName, err)
	}
	caps := []string{"readFiles", "listFiles", "listBuckets", "listFileVersions"}
	if !readOnly {
		caps = append(caps, "writeFiles", "deleteFiles")
	}
	key, err := bucket.CreateKey(ctx, name, b2.Capabilities(caps...))
	if err != nil {
		return "", "", fmt.Errorf("creating application key: %w", err)
	}
	return key.ID(), key.Secret(), nil
}

// DeleteApplicationKey deletes the application key with the given ID.
// Returns nil if the key is not found (idempotent).
func (c *Client) DeleteApplicationKey(ctx context.Context, keyID string) error {
	cursor := ""
	for {
		keys, next, err := c.b2.ListKeys(ctx, 100, cursor)
		for _, k := range keys {
			if k.ID() == keyID {
				if delErr := k.Delete(ctx); delErr != nil {
					return fmt.Errorf("deleting application key: %w", delErr)
				}
				return nil
			}
		}
		if errors.Is(err, io.EOF) || next == "" {
			return nil
		}
		if err != nil {
			return fmt.Errorf("listing application keys: %w", err)
		}
		cursor = next
	}
}

// extractRegion extracts the B2 region from an S3 endpoint URL.
// Example: "https://s3.us-west-004.backblazeb2.com" → "us-west-004"
func extractRegion(s3Endpoint string) string {
	u, err := url.Parse(s3Endpoint)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	host = strings.TrimPrefix(host, "s3.")
	host = strings.TrimSuffix(host, ".backblazeb2.com")
	return host
}

// isBucketNotEmpty reports whether err represents a non-empty bucket error.
func isBucketNotEmpty(err error) bool {
	_, msgCode, _ := base.MsgCode(err)
	if msgCode == "conflict_not_empty" {
		return true
	}
	// Fallback: check error message
	return strings.Contains(err.Error(), "not empty") || strings.Contains(err.Error(), "conflict_not_empty")
}
