package b2

import "testing"

func TestExtractRegion(t *testing.T) {
	tests := []struct {
		s3Endpoint string
		want       string
	}{
		{"https://s3.us-west-004.backblazeb2.com", "us-west-004"},
		{"https://s3.eu-central-003.backblazeb2.com", "eu-central-003"},
		{"https://s3.us-east-005.backblazeb2.com", "us-east-005"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractRegion(tt.s3Endpoint)
		if got != tt.want {
			t.Errorf("extractRegion(%q) = %q, want %q", tt.s3Endpoint, got, tt.want)
		}
	}
}
