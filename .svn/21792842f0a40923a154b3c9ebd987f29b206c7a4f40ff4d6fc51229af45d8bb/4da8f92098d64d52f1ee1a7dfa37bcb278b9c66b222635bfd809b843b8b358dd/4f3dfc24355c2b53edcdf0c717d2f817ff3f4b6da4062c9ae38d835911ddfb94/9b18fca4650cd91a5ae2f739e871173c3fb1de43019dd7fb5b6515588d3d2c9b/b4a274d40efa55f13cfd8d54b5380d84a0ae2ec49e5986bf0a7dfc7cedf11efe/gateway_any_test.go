package main

import (
	"errors"
	"testing"
)

func TestCloudDisplayGatewayBoundaries(t *testing.T) {

	tests := []struct {
		name, value, want string
	}{
		{"any", "any", "any"},
		{"concrete_gateway", "unified-88", "unified-88"},
		{"empty", "", "网关未知"},
		{"uppercase_any", "ANY", "网关未知"},
		{"padded_any", " any ", "网关未知"},
		{"missing_number", "unified-", "网关未知"},
		{"trailing_text", "unified-88-secret", "网关未知"},
		{"newline", "unified-88\nsecret", "网关未知"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cloudDisplayGateway(tc.value); got != tc.want {
				t.Fatalf("cloudDisplayGateway(%q): want %q, got %q", tc.value, tc.want, got)
			}
		})
	}
}

func TestMintUnavailableReasonMappingBoundaries(t *testing.T) {

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ticket_expired"},
		{"pending", errors.New("cloud mint pending; retry later"), "mint_pending"},
		{"busy", errors.New("cloud mint busy; retry later"), "mint_busy"},
		{"stopped", errors.New("cloud mint stopped"), "mint_stopped"},
		{"cookie", errors.New("invalid route cookie pair"), "seed_cookie_rejected"},
		{"capitalized_cookie", errors.New("Cookie rejected: secret-value"), "seed_cookie_rejected"},
		{"empty", errors.New(""), "mint_failed"},
		{"unknown", errors.New("upstream failed: access-secret"), "mint_failed"},
		{"pending_first", errors.New("pending busy stopped Cookie"), "mint_pending"},
		{"busy_before_stopped", errors.New("busy stopped cookie"), "mint_busy"},
		{"stopped_before_cookie", errors.New("stopped Cookie"), "mint_stopped"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mintUnavailableReason(tc.err); got != tc.want {
				t.Fatalf("mintUnavailableReason(%v): want %q, got %q", tc.err, tc.want, got)
			}
		})
	}
}
