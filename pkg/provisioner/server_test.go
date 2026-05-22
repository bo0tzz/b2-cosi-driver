package provisioner

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	b2client "github.com/bo0tzz/b2-cosi-driver/internal/b2"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

// mockB2 is a test double for the B2 interface.
type mockB2 struct {
	getBucketByName      func(ctx context.Context, name string) (string, string, bool, error)
	createBucket         func(ctx context.Context, name string, public bool, rules []b2client.LifecycleRule) (string, string, error)
	deleteBucket         func(ctx context.Context, bucketName string) error
	createApplicationKey func(ctx context.Context, name, bucketName string, readOnly bool) (string, string, error)
	deleteApplicationKey func(ctx context.Context, keyID string) error
}

func (m *mockB2) GetBucketByName(ctx context.Context, name string) (string, string, bool, error) {
	return m.getBucketByName(ctx, name)
}
func (m *mockB2) CreateBucket(ctx context.Context, name string, public bool, rules []b2client.LifecycleRule) (string, string, error) {
	return m.createBucket(ctx, name, public, rules)
}
func (m *mockB2) DeleteBucket(ctx context.Context, bucketName string) error {
	return m.deleteBucket(ctx, bucketName)
}
func (m *mockB2) CreateApplicationKey(ctx context.Context, name, bucketName string, readOnly bool) (string, string, error) {
	return m.createApplicationKey(ctx, name, bucketName, readOnly)
}
func (m *mockB2) DeleteApplicationKey(ctx context.Context, keyID string) error {
	return m.deleteApplicationKey(ctx, keyID)
}

// Ensure mockB2 implements B2 interface at compile time.
var _ B2 = (*mockB2)(nil)

func TestDriverCreateBucket_New(t *testing.T) {
	mock := &mockB2{
		getBucketByName: func(_ context.Context, _ string) (string, string, bool, error) {
			return "", "", false, nil
		},
		createBucket: func(_ context.Context, _ string, _ bool, _ []b2client.LifecycleRule) (string, string, error) {
			return "https://s3.us-west-004.backblazeb2.com", "us-west-004", nil
		},
	}
	srv := NewServer(mock)
	resp, err := srv.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
		Name:       "test-bucket",
		Parameters: map[string]string{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BucketId != "test-bucket" {
		t.Errorf("got BucketId %q, want %q", resp.BucketId, "test-bucket")
	}
	s3 := resp.BucketInfo.GetS3()
	if s3 == nil {
		t.Fatal("expected S3 protocol info")
	}
	if s3.Region != "us-west-004" {
		t.Errorf("got region %q, want %q", s3.Region, "us-west-004")
	}
}

func TestDriverCreateBucket_Existing(t *testing.T) {
	mock := &mockB2{
		getBucketByName: func(_ context.Context, _ string) (string, string, bool, error) {
			return "https://s3.us-west-004.backblazeb2.com", "us-west-004", true, nil
		},
	}
	srv := NewServer(mock)
	resp, err := srv.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
		Name: "existing-bucket",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BucketId != "existing-bucket" {
		t.Errorf("got BucketId %q, want %q", resp.BucketId, "existing-bucket")
	}
}

func TestDriverDeleteBucket_OK(t *testing.T) {
	mock := &mockB2{
		deleteBucket: func(_ context.Context, _ string) error { return nil },
	}
	srv := NewServer(mock)
	_, err := srv.DriverDeleteBucket(context.Background(), &cosi.DriverDeleteBucketRequest{BucketId: "test-bucket"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDriverDeleteBucket_NotEmpty(t *testing.T) {
	mock := &mockB2{
		deleteBucket: func(_ context.Context, _ string) error {
			return b2client.ErrBucketNotEmpty
		},
	}
	srv := NewServer(mock)
	_, err := srv.DriverDeleteBucket(context.Background(), &cosi.DriverDeleteBucketRequest{BucketId: "test-bucket"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("got code %v, want FailedPrecondition", status.Code(err))
	}
}

func TestDriverGrantBucketAccess(t *testing.T) {
	mock := &mockB2{
		getBucketByName: func(_ context.Context, _ string) (string, string, bool, error) {
			return "https://s3.us-west-004.backblazeb2.com", "us-west-004", true, nil
		},
		createApplicationKey: func(_ context.Context, _, _ string, _ bool) (string, string, error) {
			return "keyID123", "keySecret456", nil
		},
	}
	srv := NewServer(mock)
	resp, err := srv.DriverGrantBucketAccess(context.Background(), &cosi.DriverGrantBucketAccessRequest{
		BucketId:           "test-bucket",
		Name:               "test-access",
		AuthenticationType: cosi.AuthenticationType_Key,
		Parameters:         map[string]string{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.AccountId != "keyID123" {
		t.Errorf("got AccountId %q, want %q", resp.AccountId, "keyID123")
	}
	creds, ok := resp.Credentials["s3"]
	if !ok {
		t.Fatal("expected s3 credentials")
	}
	if creds.Secrets["accessKeyID"] != "keyID123" {
		t.Errorf("got accessKeyID %q, want %q", creds.Secrets["accessKeyID"], "keyID123")
	}
	if creds.Secrets["endpoint"] != "https://s3.us-west-004.backblazeb2.com" {
		t.Errorf("got endpoint %q, want %q", creds.Secrets["endpoint"], "https://s3.us-west-004.backblazeb2.com")
	}
}

func TestDriverGrantBucketAccess_IAMRejected(t *testing.T) {
	srv := NewServer(&mockB2{})
	_, err := srv.DriverGrantBucketAccess(context.Background(), &cosi.DriverGrantBucketAccessRequest{
		BucketId:           "test-bucket",
		Name:               "test-access",
		AuthenticationType: cosi.AuthenticationType_IAM,
	})
	if err == nil {
		t.Fatal("expected error for IAM auth type")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", status.Code(err))
	}
}

func TestDriverGrantBucketAccess_BucketNotFound(t *testing.T) {
	mock := &mockB2{
		getBucketByName: func(_ context.Context, _ string) (string, string, bool, error) {
			return "", "", false, nil
		},
	}
	srv := NewServer(mock)
	_, err := srv.DriverGrantBucketAccess(context.Background(), &cosi.DriverGrantBucketAccessRequest{
		BucketId:           "missing-bucket",
		Name:               "test-access",
		AuthenticationType: cosi.AuthenticationType_Key,
	})
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("got code %v, want NotFound", status.Code(err))
	}
}

func TestDriverRevokeBucketAccess(t *testing.T) {
	deleted := ""
	mock := &mockB2{
		deleteApplicationKey: func(_ context.Context, keyID string) error {
			deleted = keyID
			return nil
		},
	}
	srv := NewServer(mock)
	_, err := srv.DriverRevokeBucketAccess(context.Background(), &cosi.DriverRevokeBucketAccessRequest{
		BucketId:  "test-bucket",
		AccountId: "keyID123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != "keyID123" {
		t.Errorf("deleted key %q, want %q", deleted, "keyID123")
	}
}

func TestDriverRevokeBucketAccess_Idempotent(t *testing.T) {
	mock := &mockB2{
		deleteApplicationKey: func(_ context.Context, _ string) error { return nil },
	}
	srv := NewServer(mock)
	_, err := srv.DriverRevokeBucketAccess(context.Background(), &cosi.DriverRevokeBucketAccessRequest{
		BucketId:  "test-bucket",
		AccountId: "gone-key",
	})
	if err != nil {
		t.Fatalf("unexpected error on idempotent revoke: %v", err)
	}
}

// Keep a reference to ErrBucketNotEmpty to ensure the import is used.
var _ = errors.Is(b2client.ErrBucketNotEmpty, b2client.ErrBucketNotEmpty)

func TestParseLifecycleRules(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]string
		want   []b2client.LifecycleRule
	}{
		{
			name:   "unset returns nil",
			params: map[string]string{},
		},
		{
			name:   "empty string returns nil",
			params: map[string]string{"lifecycleRules": ""},
		},
		{
			name:   "keep only last version idiom",
			params: map[string]string{"lifecycleRules": `[{"daysFromHidingToDeleting":1}]`},
			want:   []b2client.LifecycleRule{{DaysFromHidingToDeleting: 1}},
		},
		{
			name:   "multiple rules with prefix",
			params: map[string]string{"lifecycleRules": `[{"fileNamePrefix":"tmp/","daysFromUploadingToHiding":7,"daysFromHidingToDeleting":1},{"fileNamePrefix":"logs/","daysFromHidingToDeleting":30}]`},
			want: []b2client.LifecycleRule{
				{FileNamePrefix: "tmp/", DaysFromUploadingToHiding: 7, DaysFromHidingToDeleting: 1},
				{FileNamePrefix: "logs/", DaysFromHidingToDeleting: 30},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLifecycleRules(tc.params)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d rules, want %d", len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("rule %d: got %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseLifecycleRules_Errors(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]string
	}{
		{"bad JSON", map[string]string{"lifecycleRules": "not json"}},
		{"rule without days", map[string]string{"lifecycleRules": `[{"fileNamePrefix":""}]`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseLifecycleRules(tc.params); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestDriverCreateBucket_LifecycleRulesPlumbed(t *testing.T) {
	var gotRules []b2client.LifecycleRule
	mock := &mockB2{
		getBucketByName: func(_ context.Context, _ string) (string, string, bool, error) {
			return "", "", false, nil
		},
		createBucket: func(_ context.Context, _ string, _ bool, rules []b2client.LifecycleRule) (string, string, error) {
			gotRules = rules
			return "https://s3.us-west-004.backblazeb2.com", "us-west-004", nil
		},
	}
	srv := NewServer(mock)
	_, err := srv.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
		Name:       "test-bucket",
		Parameters: map[string]string{"lifecycleRules": `[{"daysFromHidingToDeleting":1}]`},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []b2client.LifecycleRule{{DaysFromHidingToDeleting: 1}}
	if len(gotRules) != 1 || gotRules[0] != want[0] {
		t.Errorf("got rules %+v, want %+v", gotRules, want)
	}
}

func TestDriverCreateBucket_InvalidLifecycle(t *testing.T) {
	srv := NewServer(&mockB2{})
	_, err := srv.DriverCreateBucket(context.Background(), &cosi.DriverCreateBucketRequest{
		Name:       "test-bucket",
		Parameters: map[string]string{"lifecycleRules": "not json"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got code %v, want InvalidArgument", status.Code(err))
	}
}
