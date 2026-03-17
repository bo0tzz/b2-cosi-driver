package identity

import (
	"context"
	"testing"

	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

func TestDriverGetInfo(t *testing.T) {
	s := NewServer("b2.backblaze.com")
	resp, err := s.DriverGetInfo(context.Background(), &cosi.DriverGetInfoRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Name != "b2.backblaze.com" {
		t.Errorf("got name %q, want %q", resp.Name, "b2.backblaze.com")
	}
}
