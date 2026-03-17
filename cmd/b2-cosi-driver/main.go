package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	b2client "github.com/bo0tzz/b2-cosi-driver/internal/b2"
	"github.com/bo0tzz/b2-cosi-driver/pkg/identity"
	"github.com/bo0tzz/b2-cosi-driver/pkg/provisioner"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

const (
	driverName     = "b2.backblaze.com"
	defaultSocket  = "unix:///var/lib/cosi/cosi.sock"
)

func main() {
	klog.InitFlags(nil)
	klog.InfoS("starting B2 COSI driver", "driver", driverName)

	endpoint := envOrDefault("COSI_ENDPOINT", defaultSocket)
	keyID := requireEnv("B2_APPLICATION_KEY_ID")
	keySecret := requireEnv("B2_APPLICATION_KEY")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	b2, err := b2client.NewClient(ctx, keyID, keySecret)
	if err != nil {
		klog.Fatalf("initializing B2 client: %v", err)
	}

	socketPath := strings.TrimPrefix(endpoint, "unix://")
	// Remove existing socket file if present.
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		klog.Fatalf("removing stale socket %s: %v", socketPath, err)
	}

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		klog.Fatalf("listening on %s: %v", socketPath, err)
	}

	srv := grpc.NewServer()
	cosi.RegisterIdentityServer(srv, identity.NewServer(driverName))
	cosi.RegisterProvisionerServer(srv, provisioner.NewServer(b2))

	go func() {
		<-ctx.Done()
		klog.InfoS("shutting down gRPC server")
		srv.GracefulStop()
	}()

	klog.InfoS("serving on socket", "path", socketPath)
	if err := srv.Serve(lis); err != nil {
		klog.Fatalf("serving: %v", err)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		klog.Fatalf("required environment variable %s is not set", key)
	}
	return v
}
