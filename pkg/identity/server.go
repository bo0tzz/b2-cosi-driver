package identity

import (
	"context"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

// Server implements the COSI IdentityServer interface.
type Server struct {
	cosi.UnimplementedIdentityServer
	name string
}

// NewServer creates a new identity server with the given driver name.
func NewServer(name string) *Server {
	return &Server{name: name}
}

// DriverGetInfo returns the driver's unique name for routing by the COSI sidecar.
func (s *Server) DriverGetInfo(_ context.Context, _ *cosi.DriverGetInfoRequest) (*cosi.DriverGetInfoResponse, error) {
	return &cosi.DriverGetInfoResponse{Name: s.name}, nil
}
