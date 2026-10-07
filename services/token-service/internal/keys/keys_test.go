package keys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
)

func generateTestKeyB64(t *testing.T) string {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}

	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to marshal pkcs8: %v", err)
	}

	pemBlock := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	})

	return base64.StdEncoding.EncodeToString(pemBlock)
}

func TestNewKeyManager_Valid(t *testing.T) {
	keyB64 := generateTestKeyB64(t)

	km, err := NewKeyManager(keyB64)
	if err != nil {
		t.Fatalf("NewKeyManager returned error: %v", err)
	}

	if km.ActivePrivateKey() == nil {
		t.Fatal("expected non-nil active private key")
	}

	kid := km.ActiveKeyID()
	if kid == "" {
		t.Fatal("expected non-empty kid")
	}

	jwk := km.ActiveJWK()
	if jwk.Kty != "EC" || jwk.Crv != "P-256" || jwk.Alg != "ES256" || jwk.Use != "sig" {
		t.Fatalf("unexpected JWK fields: %+v", jwk)
	}
	if jwk.Kid != kid {
		t.Fatalf("JWK kid (%s) does not match active kid (%s)", jwk.Kid, kid)
	}

	jwks := km.JWKS()
	if len(jwks.Keys) != 1 {
		t.Fatalf("expected 1 key in JWKS, got %d", len(jwks.Keys))
	}
	if jwks.Keys[0].Kid != kid {
		t.Fatalf("expected JWKS key kid %s, got %s", kid, jwks.Keys[0].Kid)
	}
}

func TestNewKeyManager_ThumbprintDeterministic(t *testing.T) {
	keyB64 := generateTestKeyB64(t)

	km1, err := NewKeyManager(keyB64)
	if err != nil {
		t.Fatalf("first load failed: %v", err)
	}

	km2, err := NewKeyManager(keyB64)
	if err != nil {
		t.Fatalf("second load failed: %v", err)
	}

	if km1.ActiveKeyID() != km2.ActiveKeyID() {
		t.Fatalf("thumbprint mismatch: %s != %s", km1.ActiveKeyID(), km2.ActiveKeyID())
	}
}

func TestNewKeyManager_InvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty string", "", true},
		{"not base64", "not-valid-base64!@#$", true},
		{"valid base64 but not pem", base64.StdEncoding.EncodeToString([]byte("hello world")), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKeyManager(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("expected error: %v, got: %v", tt.wantErr, err)
			}
		})
	}
}
