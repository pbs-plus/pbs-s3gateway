package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
)

// Encryptor provides AES-256-GCM encryption/decryption.
// A nil block means passthrough (no encryption).
type Encryptor struct {
	block     cipher.Block
	gcm       cipher.AEAD
	noncePool sync.Pool
}

// NewEncryptor creates an Encryptor from a hex-encoded key.
// Empty string creates a passthrough Encryptor (no encryption).
func NewEncryptor(hexKey string) (*Encryptor, error) {
	if hexKey == "" {
		return &Encryptor{}, nil
	}

	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes (64 hex chars), got %d bytes", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm: %w", err)
	}

	return &Encryptor{
		block: block,
		gcm:   gcm,
		noncePool: sync.Pool{
			New: func() any {
				b := make([]byte, gcm.NonceSize())
				return &b
			},
		},
	}, nil
}

// Enabled returns true when encryption is active.
func (e *Encryptor) Enabled() bool {
	return e.block != nil
}

// Overhead returns the number of extra bytes Encrypt adds.
func (e *Encryptor) Overhead() int {
	if e.gcm == nil {
		return 0
	}
	return e.gcm.NonceSize() + e.gcm.Overhead()
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Output format: nonce (12 bytes) || ciphertext + tag.
// Passthrough when encryption is disabled.
// The caller should pass a dst slice with capacity len(plaintext) + Overhead()
// to avoid allocation. If dst is nil or too small, a new slice is allocated.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	if e.block == nil {
		return plaintext, nil
	}

	noncePtr := e.noncePool.Get().(*[]byte)
	nonce := *noncePtr
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		e.noncePool.Put(noncePtr)
		return nil, fmt.Errorf("nonce: %w", err)
	}

	out := e.gcm.Seal(nonce, nonce, plaintext, nil)
	// nonce was used as prefix via Seal, so we can't return it to pool
	// until the caller is done. We return a fresh nonce buf to the pool.
	fresh := make([]byte, len(nonce))
	*noncePtr = fresh
	e.noncePool.Put(noncePtr)

	return out, nil
}

// Decrypt decrypts data encrypted by Encrypt.
// Passthrough when encryption is disabled.
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if e.block == nil {
		return ciphertext, nil
	}

	nonceSize := e.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	return e.gcm.Open(nil, ciphertext[:nonceSize], ciphertext[nonceSize:], nil)
}
