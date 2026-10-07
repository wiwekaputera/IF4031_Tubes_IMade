package keys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
)

// JWK represents a public JSON Web Key for an Elliptic Curve key per RFC 7517 & RFC 7518.
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	Kid string `json:"kid"`
	Alg string `json:"alg,omitempty"`
	Use string `json:"use,omitempty"`
}

// JWKS represents a JSON Web Key Set per RFC 7517.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// KeyManager holds the active private signing key, its thumbprint, and JWKS public keys.
type KeyManager struct {
	activePrivateKey *ecdsa.PrivateKey
	activeKid        string
	activeJWK        JWK
	allJWKs          []JWK
}

// NewKeyManager loads the active ES256 private key from base64-encoded PEM and calculates its JWKS.
func NewKeyManager(privKeyB64 string) (*KeyManager, error) {
	if privKeyB64 == "" {
		return nil, fmt.Errorf("private key base64 string is empty")
	}

	pemBytes, err := base64.StdEncoding.DecodeString(privKeyB64)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode private key: %w", err)
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in decoded private key")
	}

	var ecdsaKey *ecdsa.PrivateKey

	// Try parsing as PKCS#8 first
	if parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		k, ok := parsedKey.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("PKCS#8 key is not an ECDSA private key")
		}
		ecdsaKey = k
	} else {
		// Fallback to SEC1 EC private key
		k, secErr := x509.ParseECPrivateKey(block.Bytes)
		if secErr != nil {
			return nil, fmt.Errorf("failed to parse private key as PKCS#8 (%v) or SEC1 (%w)", err, secErr)
		}
		ecdsaKey = k
	}

	// Validate curve is NIST P-256
	if ecdsaKey.Curve != elliptic.P256() {
		return nil, fmt.Errorf("unsupported curve %q: must be P-256", ecdsaKey.Curve.Params().Name)
	}

	jwk, err := PublicKeyToJWK(&ecdsaKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate JWK from public key: %w", err)
	}

	return &KeyManager{
		activePrivateKey: ecdsaKey,
		activeKid:        jwk.Kid,
		activeJWK:        jwk,
		allJWKs:          []JWK{jwk},
	}, nil
}

// PublicKeyToJWK converts an ECDSA P-256 public key to an RFC 7517 JWK with an RFC 7638 thumbprint.
func PublicKeyToJWK(pub *ecdsa.PublicKey) (JWK, error) {
	if pub.Curve != elliptic.P256() {
		return JWK{}, fmt.Errorf("unsupported curve: must be P-256")
	}

	const coordLen = 32
	xBytes := padBigInt(pub.X, coordLen)
	yBytes := padBigInt(pub.Y, coordLen)

	xEnc := base64.RawURLEncoding.EncodeToString(xBytes)
	yEnc := base64.RawURLEncoding.EncodeToString(yBytes)

	canonicalJSON := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, xEnc, yEnc)
	thumbprint := sha256.Sum256([]byte(canonicalJSON))
	kid := base64.RawURLEncoding.EncodeToString(thumbprint[:])

	return JWK{
		Kty: "EC",
		Crv: "P-256",
		X:   xEnc,
		Y:   yEnc,
		Kid: kid,
		Alg: "ES256",
		Use: "sig",
	}, nil
}

// ActivePrivateKey returns the active ECDSA signing key.
func (km *KeyManager) ActivePrivateKey() *ecdsa.PrivateKey {
	return km.activePrivateKey
}

// ActiveKeyID returns the RFC 7638 thumbprint of the active key.
func (km *KeyManager) ActiveKeyID() string {
	return km.activeKid
}

// ActiveJWK returns the active JWK.
func (km *KeyManager) ActiveJWK() JWK {
	return km.activeJWK
}

// JWKS returns the complete set of valid public keys for verification.
func (km *KeyManager) JWKS() JWKS {
	return JWKS{Keys: km.allJWKs}
}

// padBigInt ensures big.Int bytes are left-padded with zeros to exactly byteLen.
func padBigInt(val *big.Int, byteLen int) []byte {
	b := val.Bytes()
	if len(b) >= byteLen {
		return b[len(b)-byteLen:]
	}
	padded := make([]byte, byteLen)
	copy(padded[byteLen-len(b):], b)
	return padded
}
