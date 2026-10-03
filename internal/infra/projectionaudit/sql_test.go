package projectionaudit

import "testing"

func TestAuditServiceEnvName(t *testing.T) {
	tests := map[string]string{
		"user_service":      "USER",
		"auth_service":      "AUTH",
		"registration_saga": "REGISTRATION",
		"gig_service":       "GIG",
		"file_service":      "FILE",
		"order_service":     "ORDER",
		"chat_service":      "CHAT",
		"review_service":    "REVIEW",
		"payment_service":   "PAYMENT",
		"order_saga":        "ORDER_SAGA",
	}
	for sourceDB, want := range tests {
		if got := auditServiceEnvName(sourceDB); got != want {
			t.Errorf("auditServiceEnvName(%q) = %q, want %q", sourceDB, got, want)
		}
	}
}
