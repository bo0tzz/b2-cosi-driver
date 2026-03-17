package provisioner

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	b2client "github.com/bo0tzz/b2-cosi-driver/internal/b2"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

// B2 is the interface the provisioner uses to interact with Backblaze B2.
// Defined here to allow test mocking.
type B2 interface {
	GetBucketByName(ctx context.Context, name string) (s3Endpoint, region string, found bool, err error)
	CreateBucket(ctx context.Context, name string, public bool) (s3Endpoint, region string, err error)
	DeleteBucket(ctx context.Context, bucketName string) error
	CreateApplicationKey(ctx context.Context, name, bucketName string, readOnly bool) (keyID, keySecret string, err error)
	DeleteApplicationKey(ctx context.Context, keyID string) error
}

// Server implements the COSI ProvisionerServer interface.
type Server struct {
	cosi.UnimplementedProvisionerServer
	b2 B2
}

// NewServer creates a new provisioner server backed by the given B2 client.
func NewServer(client B2) *Server {
	return &Server{b2: client}
}

// DriverCreateBucket creates a B2 bucket for the given request.
// Idempotent: returns existing bucket info if the bucket already exists.
func (s *Server) DriverCreateBucket(ctx context.Context, req *cosi.DriverCreateBucketRequest) (*cosi.DriverCreateBucketResponse, error) {
	klog.V(4).InfoS("DriverCreateBucket", "name", req.Name)

	public := req.Parameters["bucketType"] == "public"

	s3Endpoint, region, found, err := s.b2.GetBucketByName(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "checking bucket existence: %v", err)
	}
	if !found {
		s3Endpoint, region, err = s.b2.CreateBucket(ctx, req.Name, public)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating bucket: %v", err)
		}
	}

	klog.V(2).InfoS("bucket ready", "name", req.Name, "endpoint", s3Endpoint, "region", region)
	return &cosi.DriverCreateBucketResponse{
		BucketId: req.Name,
		BucketInfo: &cosi.Protocol{
			Type: &cosi.Protocol_S3{
				S3: &cosi.S3{
					Region:           region,
					SignatureVersion: cosi.S3SignatureVersion_S3V4,
				},
			},
		},
	}, nil
}

// DriverDeleteBucket deletes the named B2 bucket.
// Returns OK if the bucket no longer exists. Returns FailedPrecondition if
// the bucket is not empty.
func (s *Server) DriverDeleteBucket(ctx context.Context, req *cosi.DriverDeleteBucketRequest) (*cosi.DriverDeleteBucketResponse, error) {
	klog.V(4).InfoS("DriverDeleteBucket", "bucketId", req.BucketId)

	if err := s.b2.DeleteBucket(ctx, req.BucketId); err != nil {
		if errors.Is(err, b2client.ErrBucketNotEmpty) {
			return nil, status.Errorf(codes.FailedPrecondition, "bucket %q is not empty; delete all objects before removing the bucket", req.BucketId)
		}
		return nil, status.Errorf(codes.Internal, "deleting bucket: %v", err)
	}

	klog.V(2).InfoS("bucket deleted", "bucketId", req.BucketId)
	return &cosi.DriverDeleteBucketResponse{}, nil
}

// DriverGrantBucketAccess creates a scoped B2 application key for the bucket
// and returns S3-compatible credentials.
func (s *Server) DriverGrantBucketAccess(ctx context.Context, req *cosi.DriverGrantBucketAccessRequest) (*cosi.DriverGrantBucketAccessResponse, error) {
	klog.V(4).InfoS("DriverGrantBucketAccess", "bucketId", req.BucketId, "name", req.Name)

	if req.AuthenticationType == cosi.AuthenticationType_IAM {
		return nil, status.Errorf(codes.InvalidArgument, "IAM authentication is not supported by the B2 driver; use Key authentication")
	}

	readOnly := req.Parameters["accessMode"] == "ro" || req.Parameters["accessMode"] == "read"

	s3Endpoint, region, found, err := s.b2.GetBucketByName(ctx, req.BucketId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "looking up bucket: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "bucket %q not found", req.BucketId)
	}

	keyID, keySecret, err := s.b2.CreateApplicationKey(ctx, req.Name, req.BucketId, readOnly)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "creating application key: %v", err)
	}

	klog.V(2).InfoS("access granted", "bucketId", req.BucketId, "keyId", keyID, "readOnly", readOnly)
	return &cosi.DriverGrantBucketAccessResponse{
		AccountId: keyID,
		Credentials: map[string]*cosi.CredentialDetails{
			"s3": {
				Secrets: map[string]string{
					"accessKeyID":     keyID,
					"accessSecretKey": keySecret,
					"endpoint":        s3Endpoint,
					"region":          region,
				},
			},
		},
	}, nil
}

// DriverRevokeBucketAccess deletes the B2 application key identified by
// req.AccountId (the key ID returned by DriverGrantBucketAccess).
// Returns OK if the key no longer exists.
func (s *Server) DriverRevokeBucketAccess(ctx context.Context, req *cosi.DriverRevokeBucketAccessRequest) (*cosi.DriverRevokeBucketAccessResponse, error) {
	klog.V(4).InfoS("DriverRevokeBucketAccess", "bucketId", req.BucketId, "accountId", req.AccountId)

	if err := s.b2.DeleteApplicationKey(ctx, req.AccountId); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting application key: %v", err)
	}

	klog.V(2).InfoS("access revoked", "keyId", req.AccountId)
	return &cosi.DriverRevokeBucketAccessResponse{}, nil
}
