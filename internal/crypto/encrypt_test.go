package crypto

import (
	"encoding/hex"
	"testing"
)

func TestPassthrough(t *testing.T) {
	enc, err := NewEncryptor("")
	if err != nil {
		t.Fatal(err)
	}
	if enc.Enabled() {
		t.Error("empty key should not enable encryption")
	}

	data := []byte("hello world")
	encrypted, err := enc.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(encrypted) != string(data) {
		t.Errorf("passthrough Encrypt should return original data")
	}

	decrypted, err := enc.Decrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(data) {
		t.Errorf("passthrough Decrypt should return original data")
	}
}

func TestRoundTrip(t *testing.T) {
	key := hex.EncodeToString(make([]byte, 32)) // 64 hex chars = 32 bytes
	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	if !enc.Enabled() {
		t.Error("non-empty key should enable encryption")
	}

	plaintext := []byte("hello world")
	ciphertext, err := enc.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if string(ciphertext) == string(plaintext) {
		t.Error("ciphertext should differ from plaintext")
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted = %q, want %q", string(decrypted), string(plaintext))
	}
}

func TestRoundTripEmptyData(t *testing.T) {
	key := hex.EncodeToString(make([]byte, 32))
	enc, _ := NewEncryptor(key)

	ciphertext, err := enc.Encrypt([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := enc.Decrypt(ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if len(decrypted) != 0 {
		t.Errorf("decrypted empty data should be empty, got %d bytes", len(decrypted))
	}
}

func TestInvalidHexKey(t *testing.T) {
	_, err := NewEncryptor("not-valid-hex!")
	if err == nil {
		t.Error("expected error for invalid hex key")
	}
}

func TestWrongKeyFails(t *testing.T) {
	key1 := hex.EncodeToString(bytes32(1))
	key2 := hex.EncodeToString(bytes32(2))

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	ciphertext, _ := enc1.Encrypt([]byte("secret"))
	_, err := enc2.Decrypt(ciphertext)
	if err == nil {
		t.Error("decryption with wrong key should fail")
	}
}

func TestDifferentNoncePerEncrypt(t *testing.T) {
	key := hex.EncodeToString(make([]byte, 32))
	enc, _ := NewEncryptor(key)

	data := []byte("same data")
	ct1, _ := enc.Encrypt(data)
	ct2, _ := enc.Encrypt(data)

	if string(ct1) == string(ct2) {
		t.Error("two encryptions of the same data should produce different ciphertext (random nonce)")
	}

	// Both should still decrypt correctly
	d1, _ := enc.Decrypt(ct1)
	d2, _ := enc.Decrypt(ct2)
	if string(d1) != string(data) || string(d2) != string(data) {
		t.Error("both ciphertexts should decrypt to the same plaintext")
	}
}

func TestInvalidKeyLength(t *testing.T) {
	// 16 bytes = AES-128, not AES-256
	shortKey := hex.EncodeToString(make([]byte, 16))
	_, err := NewEncryptor(shortKey)
	if err == nil {
		t.Error("expected error for non-32-byte key")
	}
}

func bytes32(b byte) []byte {
	buf := make([]byte, 32)
	for i := range buf {
		buf[i] = b
	}
	return buf
}
