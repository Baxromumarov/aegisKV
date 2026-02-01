package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewTLSConfigNoCert(t *testing.T) {
	cfg, err := NewTLSConfig(Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config when no cert provided")
	}
}

func TestNewTLSConfigMissingKey(t *testing.T) {
	cfg, err := NewTLSConfig(Config{
		CertFile: "cert.pem",
		// KeyFile missing
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config when key is missing")
	}
}

func TestNewTLSConfigInvalidCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	os.WriteFile(certFile, []byte("invalid cert"), 0600)
	os.WriteFile(keyFile, []byte("invalid key"), 0600)

	_, err := NewTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
	})

	if err == nil {
		t.Error("expected error for invalid cert")
	}
}

func TestNewTLSConfigValidCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	cfg, err := NewTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Error("expected TLS 1.3 minimum")
	}
	if len(cfg.Certificates) != 1 {
		t.Error("expected 1 certificate")
	}
}

func TestNewTLSConfigWithCA(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	caFile := filepath.Join(dir, "ca.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}
	// Copy cert as CA
	certData, _ := os.ReadFile(certFile)
	os.WriteFile(caFile, certData, 0600)

	cfg, err := NewTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.ClientCAs == nil {
		t.Error("expected ClientCAs to be set")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Error("expected mTLS client auth")
	}
}

func TestNewTLSConfigInvalidCA(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	caFile := filepath.Join(dir, "ca.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}
	os.WriteFile(caFile, []byte("invalid ca"), 0600)

	_, err := NewTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	})

	if err == nil {
		t.Error("expected error for invalid CA")
	}
}

func TestNewTLSConfigCAFileNotFound(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	_, err := NewTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   "/nonexistent/ca.pem",
	})

	if err == nil {
		t.Error("expected error for missing CA file")
	}
}

func TestNewClientTLSConfigEmpty(t *testing.T) {
	cfg, err := NewClientTLSConfig(Config{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Error("expected TLS 1.3 minimum")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify when no CA")
	}
}

func TestNewClientTLSConfigWithCA(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}
	certData, _ := os.ReadFile(certFile)
	os.WriteFile(caFile, certData, 0600)

	cfg, err := NewClientTLSConfig(Config{
		CAFile: caFile,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false with CA")
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs should be set")
	}
}

func TestNewClientTLSConfigWithClientCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	if err := generateTestCert(certFile, keyFile); err != nil {
		t.Fatalf("failed to generate test cert: %v", err)
	}

	cfg, err := NewClientTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Error("expected 1 client certificate")
	}
}

func TestNewClientTLSConfigInvalidClientCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	os.WriteFile(certFile, []byte("invalid"), 0600)
	os.WriteFile(keyFile, []byte("invalid"), 0600)

	_, err := NewClientTLSConfig(Config{
		CertFile: certFile,
		KeyFile:  keyFile,
	})

	if err == nil {
		t.Error("expected error for invalid client cert")
	}
}

func TestNewClientTLSConfigInvalidCA(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")

	os.WriteFile(caFile, []byte("invalid ca"), 0600)

	_, err := NewClientTLSConfig(Config{
		CAFile: caFile,
	})

	if err == nil {
		t.Error("expected error for invalid CA")
	}
}

// generateTestCert generates a self-signed certificate for testing
func generateTestCert(certFile, keyFile string) error {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		return err
	}

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		return err
	}

	return nil
}
