package secretbox

import "testing"

func TestEncryptDecrypt(t *testing.T) {
	ciphertext, err := Encrypt("sk-example", "strong-config-key")
	if err != nil {
		t.Fatal(err)
	}
	if ciphertext == "sk-example" || ciphertext == "" {
		t.Fatal("plaintext must not be persisted")
	}
	plaintext, err := Decrypt(ciphertext, "strong-config-key")
	if err != nil || plaintext != "sk-example" {
		t.Fatalf("unexpected decrypt result: %q, %v", plaintext, err)
	}
}
