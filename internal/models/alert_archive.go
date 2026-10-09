package models

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// A small transaction receipt avoids adding a unique index to the existing
// potentially large history table. Receipt and history are committed together.
// Receipts must not be deleted independently of the corresponding history.
type AlertArchiveReceipt struct {
	ArchiveKey string `gorm:"primaryKey;size:64"`
	Owner      string `gorm:"size:32;not null"`
}

var ErrInvalidRecovery = errors.New("invalid recovered event identity or timestamps")

func (event AlertHisEvent) ArchiveIdentity() (string, error) {
	if event.TenantId == "" || event.FaultCenterId == "" || event.Fingerprint == "" || event.FirstTriggerTime <= 0 || event.RecoverTime < event.FirstTriggerTime {
		return "", ErrInvalidRecovery
	}
	// Legacy blank event IDs remain identifiable by their first trigger time.
	// Do not include mutable labels, notification clocks or recovery time: these
	// must not turn a retry of the same incident into a second history record.
	encoded, err := json.Marshal([]any{"v1", event.TenantId, event.FaultCenterId, event.Fingerprint, event.RuleId, event.EventId, event.FirstTriggerTime})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
