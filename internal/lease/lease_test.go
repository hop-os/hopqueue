package lease

import (
	"strings"
	"testing"

	"github.com/xinix00/hoplock/mem"
)

func TestParseLeaseURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in         string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{"s3://my-bucket/lease.json", "my-bucket", "lease.json", false},
		{"s3://my-bucket/queue/lease.json", "my-bucket", "queue/lease.json", false},
		{"s3://my-bucket/", "", "", true},
		{"s3://my-bucket", "", "", true},
		{"https://my-bucket/lease.json", "", "", true},
		{"", "", "", true},
		{"s3://my-bucket/lease.json?versionId=1", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			b, k, err := parseLeaseURL(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("parseLeaseURL(%q): want error, got bucket=%q key=%q", c.in, b, k)
				}
				return
			}
			if err != nil {
				t.Errorf("parseLeaseURL(%q): unexpected error %v", c.in, err)
			}
			if b != c.wantBucket {
				t.Errorf("bucket = %q, want %q", b, c.wantBucket)
			}
			if k != c.wantKey {
				t.Errorf("key = %q, want %q", k, c.wantKey)
			}
		})
	}
}

func TestNew_S3Path_RequiredFields(t *testing.T) {
	t.Parallel()
	base := Config{
		NodeID:          "node-1",
		LeaseURL:        "s3://bucket/lease.json",
		Region:          "us-east-1",
		AccessKeyID:     "AKID",
		SecretAccessKey: "secret",
	}

	if _, err := New(base); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	type mut func(*Config)
	cases := []struct {
		name   string
		mutate mut
		want   string
	}{
		{"missing NodeID", func(c *Config) { c.NodeID = "" }, "NodeID"},
		{"missing LeaseURL", func(c *Config) { c.LeaseURL = "" }, "LeaseURL"},
		{"missing Region", func(c *Config) { c.Region = "" }, "Region"},
		{"missing AccessKeyID", func(c *Config) { c.AccessKeyID = "" }, "AccessKeyID"},
		{"missing SecretAccessKey", func(c *Config) { c.SecretAccessKey = "" }, "SecretAccessKey"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base
			c.mutate(&cfg)
			_, err := New(cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

func TestNew_BackendOverride_SkipsS3Validation(t *testing.T) {
	t.Parallel()
	// With Backend set, none of the S3 fields are required.
	_, err := New(Config{
		NodeID:  "node-1",
		Backend: mem.New(),
	})
	if err != nil {
		t.Fatalf("Backend override: %v", err)
	}
}

func TestNew_BackendOverride_StillRequiresNodeID(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Backend: mem.New()})
	if err == nil {
		t.Fatal("expected error for missing NodeID")
	}
	if !strings.Contains(err.Error(), "NodeID") {
		t.Errorf("error %q does not mention NodeID", err)
	}
}

func TestElector_InitialStateNotLeader(t *testing.T) {
	t.Parallel()
	e, err := New(Config{NodeID: "node-1", Backend: mem.New()})
	if err != nil {
		t.Fatal(err)
	}
	if e.IsLeader() {
		t.Error("fresh Elector reports IsLeader=true")
	}
	if e.CurrentGeneration() != 0 {
		t.Errorf("fresh CurrentGeneration = %d, want 0", e.CurrentGeneration())
	}
}
