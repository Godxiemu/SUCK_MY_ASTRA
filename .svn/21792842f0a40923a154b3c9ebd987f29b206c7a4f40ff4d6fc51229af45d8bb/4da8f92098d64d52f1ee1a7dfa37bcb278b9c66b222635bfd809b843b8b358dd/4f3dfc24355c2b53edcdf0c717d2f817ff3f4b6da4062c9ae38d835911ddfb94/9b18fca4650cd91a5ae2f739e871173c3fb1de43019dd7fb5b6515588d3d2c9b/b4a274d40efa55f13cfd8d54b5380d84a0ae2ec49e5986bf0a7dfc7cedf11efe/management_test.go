package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
)

const (
	mgmtStatusPath   = "/v0/management/codex-turn-state/status"
	mgmtClearPath    = "/v0/management/codex-turn-state/buckets/clear"
	mgmtSelftestPath = "/v0/management/codex-turn-state/selftest"
	mgmtConfigPath   = "/v0/management/codex-turn-state/config"
	mgmtResourcePath = "/v0/resource/plugins/codex-turn-state/"

	opsProbeStartPath  = mgmtResourcePath + "ops/probe/start"
	opsProbeCancelPath = mgmtResourcePath + "ops/probe/cancel"

	opsChoicesPath = mgmtResourcePath + "ops/choices"
)

type mgmtResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers"`
	Body       []byte      `json:"Body"`
}

type mgmtRoute struct {
	Method      string `json:"Method"`
	Path        string `json:"Path"`
	Menu        string `json:"Menu"`
	Description string `json:"Description"`
}

type mgmtRegistration struct {
	Routes    []mgmtRoute `json:"routes"`
	Resources []mgmtRoute `json:"resources"`
}

type mgmtBucket struct {
	AuthID      string `json:"auth_id"`
	Model       string `json:"model"`
	Ready       bool   `json:"ready"`
	Len         int    `json:"len"`
	Enabled     bool   `json:"enabled"`
	IssuedAt    string `json:"issued_at"`
	ExpiresAt   string `json:"expires_at"`
	SecondsLeft int64  `json:"seconds_left"`

	Observed *mgmtObserved `json:"observed"`
}

type mgmtObserved struct {
	NaturalNormal   int64  `json:"natural_normal"`
	NaturalLimited  int64  `json:"natural_limited"`
	InjectedSilent  int64  `json:"injected_silent"`
	InjectedLimited int64  `json:"injected_limited"`
	LastKind        string `json:"last_kind"`
	LastWrote       bool   `json:"last_wrote"`
	LastNaturalKind string `json:"last_natural_kind"`
	LastNaturalAt   string `json:"last_natural_at"`
}

type mgmtObservationEvent struct {
	AuthID string `json:"auth_id"`
	Model  string `json:"model"`
	Len    int    `json:"len"`
	Wrote  bool   `json:"wrote"`
	Kind   string `json:"kind"`
}

type mgmtCounters struct {
	Harvest int64 `json:"harvest"`
	Steer   int64 `json:"steer"`
	Pass    int64 `json:"pass"`
	Skip    int64 `json:"skip"`
}

type mgmtStatus struct {
	Role           string       `json:"role"`
	DryRun         bool         `json:"dry_run"`
	TTLSeconds     int          `json:"ttl_seconds"`
	TemplateLength int          `json:"template_length"`
	ReplaceLength  int          `json:"replace_length"`
	StoreDir       string       `json:"store_dir"`
	Models         []string     `json:"models"`
	Buckets        []mgmtBucket `json:"buckets"`
	TargetsTotal   int          `json:"targets_total"`
	TargetsReady   int          `json:"targets_ready"`

	AccountsSource    string                 `json:"accounts_source"`
	AccountsError     string                 `json:"accounts_error"`
	StoreError        string                 `json:"store_error"`
	Counters          mgmtCounters           `json:"counters"`
	ObservationsSince string                 `json:"observations_since"`
	ObservationFeed   []mgmtObservationEvent `json:"observation_feed"`
	ProbeAccounts     []string               `json:"probe_accounts"`

	ProbeProxyCount int      `json:"probe_proxy_count"`
	ProbeProxies    []string `json:"probe_proxies"`
}

type mgmtClearResult struct {
	Cleared int `json:"cleared"`
}

type mgmtSelftestResult struct {
	Reached    bool   `json:"reached"`
	StatusCode int    `json:"status_code"`
	Model      string `json:"model"`
	AuthID     string `json:"auth_id"`
	Targeted   bool   `json:"targeted"`
	Harvested  bool   `json:"harvested"`
	Note       string `json:"note"`
	Error      string `json:"error"`
}

func decodeMgmtEnvelope(t *testing.T, raw []byte) json.RawMessage {
	t.Helper()
	var env struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode envelope: %v (raw: %s)", err, truncateMgmtLog(raw))
	}
	if !env.OK {
		t.Fatalf("plugin returned an error envelope: %+v", env.Error)
	}
	return env.Result
}

func truncateMgmtLog(raw []byte) string {
	const limit = 200
	if len(raw) <= limit {
		return string(raw)
	}
	return string(raw[:limit]) + "..."
}

func driveManagement(t *testing.T, method, path string, body []byte) mgmtResponse {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"Method":  method,
		"Path":    path,
		"Headers": http.Header{},
		"Query":   url.Values{},
		"Body":    body,
	})
	if err != nil {
		t.Fatalf("marshal management request: %v", err)
	}
	out, errHandle := handleMethod(pluginabi.MethodManagementHandle, raw)
	if errHandle != nil {
		t.Fatalf("handleMethod(management.handle) %s %s: %v", method, path, errHandle)
	}
	var resp mgmtResponse
	if result := decodeMgmtEnvelope(t, out); len(result) > 0 {
		if errUnmarshal := json.Unmarshal(result, &resp); errUnmarshal != nil {
			t.Fatalf("decode management response: %v", errUnmarshal)
		}
	}

	if resp.StatusCode == 0 {
		resp.StatusCode = http.StatusOK
	}
	return resp
}

func driveManagementJSON(t *testing.T, method, path string, payload any) mgmtResponse {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return driveManagement(t, method, path, body)
}

func driveManagementRegister(t *testing.T) mgmtRegistration {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"Plugin":           map[string]any{"Name": "codex-turn-state"},
		"BasePath":         "/v0/management",
		"ResourceBasePath": "/v0/resource/plugins/codex-turn-state",
	})
	if err != nil {
		t.Fatalf("marshal management registration request: %v", err)
	}
	out, errHandle := handleMethod(pluginabi.MethodManagementRegister, raw)
	if errHandle != nil {
		t.Fatalf("handleMethod(management.register): %v", errHandle)
	}
	var reg mgmtRegistration
	if result := decodeMgmtEnvelope(t, out); len(result) > 0 {
		if errUnmarshal := json.Unmarshal(result, &reg); errUnmarshal != nil {
			t.Fatalf("decode management registration: %v", errUnmarshal)
		}
	}
	return reg
}

func mustManagementStatus(t *testing.T) mgmtStatus {
	t.Helper()
	resp := driveManagement(t, http.MethodGet, mgmtStatusPath, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status returned %d, want 200 (body: %s)", resp.StatusCode, truncateMgmtLog(resp.Body))
	}
	var status mgmtStatus
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		t.Fatalf("decode status: %v (body: %s)", err, truncateMgmtLog(resp.Body))
	}
	return status
}

func probeRoleConfig(dir string) string {
	return fmt.Sprintf(`role: probe
store_dir: %q
template_length: 292
replace_length: 312
ttl_seconds: 3600
dry_run: true
log_decisions: false
models:
  - gpt-5.5
  - gpt-5.6-sol
`, dir)
}

func businessConfigWithModels(dir string) string {
	return fmt.Sprintf(`role: business
store_dir: %q
template_length: 292
replace_length: 312
ttl_seconds: 3600
dry_run: true
log_decisions: false
models:
  - gpt-5.5
  - gpt-5.6-sol
`, dir)
}

func probeRoleConfigModels(dir string, models ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `role: probe
store_dir: %q
template_length: 292
replace_length: 312
ttl_seconds: 3600
dry_run: true
log_decisions: false
models:
`, dir)
	for _, model := range models {
		fmt.Fprintf(&b, "  - %s\n", model)
	}
	return b.String()
}

func seedMgmtBucket(t *testing.T, dir, authID, model string, issued time.Time) string {
	t.Helper()
	recordObservation(defaultConfig(), authID, model, 292, false)
	return fakeToken(292, issued)
}

func observedCellCount() int {
	cells, _, _ := observationsSnapshot()
	return len(cells)
}

func mgmtBucketByKey(status mgmtStatus, authID, model string) (mgmtBucket, bool) {
	for _, bucket := range status.Buckets {
		if bucket.AuthID == authID && bucket.Model == model {
			return bucket, true
		}
	}
	return mgmtBucket{}, false
}

func TestManagementStatusNeverLeaksTokenValues(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	issued := wallClock().Add(-5 * time.Minute)
	secrets := []string{
		seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", issued),
		seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.6-sol", issued),
		seedMgmtBucket(t, dir, "codex-beta.json", "gpt-5.5", issued),
	}

	resp := driveManagement(t, http.MethodGet, mgmtStatusPath, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status returned %d, want 200", resp.StatusCode)
	}
	body := string(resp.Body)
	for i, secret := range secrets {

		needle := secret[:40]
		if strings.Contains(body, needle) {
			t.Errorf("status body leaked token %d", i)
		}
	}

	if len(resp.Body) == 0 {
		t.Fatal("status body is empty; the leak assertions above proved nothing")
	}

	status := mustManagementStatus(t)
	for _, want := range [][2]string{
		{"codex-alpha.json", "gpt-5.5"},
		{"codex-alpha.json", "gpt-5.6-sol"},
		{"codex-beta.json", "gpt-5.5"},
	} {
		bucket, ok := mgmtBucketByKey(status, want[0], want[1])
		if !ok || !bucket.Ready {
			t.Fatalf("seeded bucket %s/%s is not reported ready; the leak assertions above were not exercised against real records", want[0], want[1])
		}
	}
}

func TestResourceShellIsStaticAndDataFree(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	before := driveManagement(t, http.MethodGet, mgmtResourcePath, nil)
	if before.StatusCode != http.StatusOK {
		t.Fatalf("resource shell returned %d, want 200", before.StatusCode)
	}
	if len(before.Body) == 0 {
		t.Fatal("resource shell body is empty; nothing was actually served")
	}

	issued := wallClock().Add(-time.Minute)
	secret := seedMgmtBucket(t, dir, "codex-secret-account.json", "gpt-5.6-sol", issued)

	after := driveManagement(t, http.MethodGet, mgmtResourcePath, nil)
	if string(before.Body) != string(after.Body) {
		t.Error("resource shell changed after the store changed; it is rendering runtime state on an unauthenticated route")
	}

	shell := string(after.Body)
	for _, forbidden := range []string{
		secret[:40],
		"codex-secret-account.json",
		dir,
	} {
		if strings.Contains(shell, forbidden) {
			t.Errorf("resource shell embedded runtime data: %q", truncateMgmtLog([]byte(forbidden)))
		}
	}
}

func TestRegistrationAdvertisesManagementAPI(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	raw, err := json.Marshal(map[string]any{
		"config_yaml":    []byte(probeRoleConfig(dir)),
		"schema_version": 6,
	})
	if err != nil {
		t.Fatalf("marshal register request: %v", err)
	}
	out, errHandle := handleMethod(pluginabi.MethodPluginRegister, raw)
	if errHandle != nil {
		t.Fatalf("handleMethod(plugin.register): %v", errHandle)
	}
	var reg struct {
		Capabilities map[string]any `json:"capabilities"`
	}
	if result := decodeMgmtEnvelope(t, out); len(result) > 0 {
		if errUnmarshal := json.Unmarshal(result, &reg); errUnmarshal != nil {
			t.Fatalf("decode registration: %v", errUnmarshal)
		}
	}
	enabled, ok := reg.Capabilities["management_api"].(bool)
	if !ok {
		t.Fatalf("registration does not declare management_api at all; capabilities: %v", reg.Capabilities)
	}
	if !enabled {
		t.Error("management_api is false; the management routes will never be mounted")
	}
}

func TestManagementDataRoutesCarryNoMenu(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	reg := driveManagementRegister(t)
	if len(reg.Routes) == 0 {
		t.Fatal("no management routes declared; this test would pass vacuously")
	}
	for _, route := range reg.Routes {
		if strings.TrimSpace(route.Menu) != "" {
			t.Errorf("management route %s %s carries Menu=%q, which demotes it to the unauthenticated resource prefix",
				route.Method, route.Path, route.Menu)
		}
	}
}

func TestManagementRegisterExposesExactlyOneMenuResource(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	reg := driveManagementRegister(t)
	if len(reg.Resources) == 0 {
		t.Fatal("no resources declared; this test would pass vacuously")
	}

	var withMenu []mgmtRoute
	for _, res := range reg.Resources {
		if strings.TrimSpace(res.Menu) != "" {
			withMenu = append(withMenu, res)
		}
	}
	if len(withMenu) != 1 {
		t.Fatalf("declared %d resources with a Menu, want exactly 1 (the shell); the rest must stay off the menu", len(withMenu))
	}
	if withMenu[0].Path != "/dashboard" {
		t.Errorf("the menu-bearing resource is %q, want /dashboard", withMenu[0].Path)
	}
}

func TestManagementRegisterExposesAnonymousStatusResource(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	reg := driveManagementRegister(t)
	var status *mgmtRoute
	for i := range reg.Resources {
		if reg.Resources[i].Path == "/status" {
			status = &reg.Resources[i]
			break
		}
	}
	if status == nil {
		t.Fatal("no /status resource declared; the dashboard has nothing to fetch from")
	}
	if strings.TrimSpace(status.Menu) != "" {
		t.Errorf("the /status resource carries Menu=%q; it is a fetch target, not a page", status.Menu)
	}
}

func TestAnonymousStatusResourceNeverLeaksTokenValues(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	issued := wallClock().Add(-time.Minute)
	secrets := []string{
		seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", issued),
		seedMgmtBucket(t, dir, "codex-beta.json", "gpt-5.6-sol", issued),
	}

	resp := driveManagement(t, http.MethodGet, "/v0/resource/plugins/codex-turn-state/status", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous status returned %d, want 200: %s", resp.StatusCode, truncateMgmtLog(resp.Body))
	}
	if len(resp.Body) == 0 {
		t.Fatal("anonymous status body is empty; the leak check below would prove nothing")
	}
	for i, secret := range secrets {
		if strings.Contains(string(resp.Body), secret[:40]) {
			t.Errorf("anonymous status leaked token %d", i)
		}
	}

	var status mgmtStatus
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		t.Fatalf("anonymous status body is not a status document: %v", err)
	}
	if b, ok := mgmtBucketByKey(status, "codex-alpha.json", "gpt-5.5"); !ok || !b.Ready {
		t.Error("seeded bucket missing from anonymous status; leak check saw no real data")
	}
}

func TestClearBucketRejectsPathTraversal(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "store")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}

	sentinel := filepath.Join(base, "sentinel.json")
	if err := os.WriteFile(sentinel, []byte("untouched"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	mustConfigure(t, probeRoleConfig(dir))
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", wallClock().Add(-time.Minute))

	cases := []struct {
		name   string
		authID string
		model  string
	}{
		{"parent via auth", "..", "sentinel"},
		{"parent via model", "codex-alpha.json", "../sentinel"},
		{"nested parent", "../..", "sentinel"},
		{"slash in auth", "codex/../..", "sentinel"},
		{"backslash in auth", `..\..`, "sentinel"},
		{"absolute model", "codex-alpha.json", filepath.ToSlash(sentinel)},
		{"empty auth", "", "gpt-5.5"},
		{"empty model", "codex-alpha.json", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := driveManagementJSON(t, http.MethodPost, mgmtClearPath, map[string]any{
				"auth_id": tc.authID,
				"model":   tc.model,
			})
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				t.Errorf("traversal accepted: status %d, want 4xx", resp.StatusCode)
			}
			if _, err := os.Stat(sentinel); err != nil {
				t.Fatalf("sentinel outside the store was removed: %v", err)
			}
		})
	}

	if _, ok := mgmtBucketByKey(mustManagementStatus(t), "codex-alpha.json", "gpt-5.5"); !ok {
		t.Fatal("the ordinary bucket was collateral damage")
	}
}

func TestClearBucketRejectsMalformedBody(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", wallClock().Add(-time.Minute))
	before := observedCellCount()

	cases := []struct {
		name string
		body []byte
	}{
		{"not json", []byte("this is not json")},
		{"empty body", nil},
		{"empty object", []byte(`{}`)},
		{"model without auth", []byte(`{"model":"gpt-5.5"}`)},
		{"auth without model", []byte(`{"auth_id":"codex-alpha.json"}`)},
		{"all and auth together", []byte(`{"all":true,"auth_id":"codex-alpha.json","model":"gpt-5.5"}`)},
		{"json array", []byte(`["codex-alpha.json"]`)},
		{"wrong types", []byte(`{"auth_id":42,"model":true}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := driveManagement(t, http.MethodPost, mgmtClearPath, tc.body)
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				t.Errorf("malformed body accepted: status %d, want 4xx", resp.StatusCode)
			}
		})
	}
	if observedCellCount() != before {
		t.Error("a rejected clear still modified the tally")
	}
}

func TestSelftestWorksRegardlessOfRole(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(dir string) string
	}{
		{"probe", probeRoleConfig},
		{"business", func(dir string) string { return businessConfigWithModels(dir) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			mustConfigure(t, tc.cfg(dir))

			resp := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{"model": "gpt-5.5"})

			if resp.StatusCode == http.StatusConflict {
				t.Errorf("selftest refused with 409 under role %s: %s", tc.name, truncateMgmtLog(resp.Body))
			}
			if strings.Contains(strings.ToLower(string(resp.Body)), "role") {
				t.Errorf("selftest under role %s blamed the role: %s", tc.name, truncateMgmtLog(resp.Body))
			}
		})
	}
}

func TestSelftestRejectsUnconfiguredModel(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	resp := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{"model": "gpt-4-turbo"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("selftest with an unconfigured model returned %d, want 400", resp.StatusCode)
	}

	next := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{"model": "gpt-5.5"})
	if next.StatusCode == http.StatusBadRequest {
		t.Error("a configured model was also rejected as unconfigured")
	}
}

func TestSelftestFailsClosedWithoutHostAPI(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	resp := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{"model": "gpt-5.5"})

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("selftest with no host API returned %d, want 503: %s", resp.StatusCode, truncateMgmtLog(resp.Body))
	}

	var result mgmtSelftestResult
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("decode selftest result: %v (body: %s)", err, truncateMgmtLog(resp.Body))
	}
	if result.Reached {
		t.Error("selftest claimed it reached upstream with no host API available")
	}
	if result.Harvested {
		t.Error("selftest claimed a harvest; it structurally cannot harvest")
	}
	if strings.TrimSpace(result.Error) == "" {
		t.Error("the 503 carries no explanation, leaving the operator with no reason")
	}
	if n := poolEntryCount(); n != 0 {
		t.Errorf("a failed selftest still pooled %d entries", n)
	}
}

func TestSelftestRejectsMalformedBody(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	cases := []struct {
		name string
		body []byte
	}{
		{"not json", []byte("nope")},
		{"empty body", nil},
		{"no model", []byte(`{}`)},
		{"empty model", []byte(`{"model":""}`)},
		{"wrong type", []byte(`{"model":42}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := driveManagement(t, http.MethodPost, mgmtSelftestPath, tc.body)
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				t.Errorf("malformed selftest body accepted: status %d, want 4xx", resp.StatusCode)
			}
		})
	}
}

func TestSelftestRejectsUnsafeAuthID(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	cases := []struct {
		name   string
		authID string
	}{
		{"parent", ".."},
		{"nested parent", "../.."},
		{"slash", "codex/../.."},
		{"backslash", `..\..`},
		{"leading slash", "/etc/passwd"},
		{"embedded null", "codex\x00.json"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{
				"model":   "gpt-5.5",
				"auth_id": tc.authID,
			})

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("unsafe auth_id %q returned %d, want 400: %s",
					tc.authID, resp.StatusCode, truncateMgmtLog(resp.Body))
			}
		})
	}

	ok := driveManagementJSON(t, http.MethodPost, mgmtSelftestPath, map[string]any{
		"model":   "gpt-5.5",
		"auth_id": "codex-alpha.json",
	})
	if ok.StatusCode == http.StatusBadRequest {
		t.Errorf("a well-formed auth_id was rejected as unsafe: %s", truncateMgmtLog(ok.Body))
	}

}

func managementFuncBody(t *testing.T, name string) string {
	t.Helper()
	src, err := os.ReadFile("management.go")
	if err != nil {
		t.Fatalf("read management.go: %v", err)
	}
	text := string(src)
	start := strings.Index(text, "func "+name+"(")
	if start < 0 {
		t.Fatalf("management.go has no func %s; the selftest contract requires it", name)
	}

	end := strings.Index(text[start:], "\n}")
	if end < 0 {
		t.Fatalf("could not find the end of func %s", name)
	}
	return text[start : start+end]
}

func TestSelftestAuthIDIsNeverFabricated(t *testing.T) {
	body := managementFuncBody(t, "runSelftest")

	assignments := 0
	for idx := 0; ; {
		at := strings.Index(body[idx:], "AuthID")
		if at < 0 {
			break
		}
		at += idx
		idx = at + len("AuthID")

		rest := strings.TrimLeft(body[idx:], " \t")
		var value string
		switch {
		case strings.HasPrefix(rest, ":") && !strings.HasPrefix(rest, ":="):
			value = strings.TrimLeft(rest[1:], " \t")
		case strings.HasPrefix(rest, "="):
			value = strings.TrimLeft(rest[1:], " \t")
		default:
			continue
		}
		assignments++

		if !strings.HasPrefix(value, "authID") {
			line := strings.Count(body[:at], "\n")
			t.Errorf("runSelftest assigns AuthID from %q (about %d lines into the function); "+
				"it must come from the caller-supplied authID, because CPA reports no credential on the response",
				truncateMgmtLog([]byte(value[:min(50, len(value))])), line)
		}
	}
	if assignments == 0 {
		t.Error("no assignment to AuthID found in runSelftest; this test would pass vacuously")
	}
}

func TestSelftestEchoesTargetingHonestly(t *testing.T) {
	body := managementFuncBody(t, "runSelftest")

	at := strings.Index(body, "selftestResponse{")
	if at < 0 {
		t.Fatal("runSelftest builds no selftestResponse; this test would pass vacuously")
	}
	end := strings.Index(body[at:], "\n\t}")
	if end < 0 {
		t.Fatal("could not find the end of the selftestResponse literal")
	}
	literal := body[at : at+end]

	if !strings.Contains(literal, "AuthID:") {
		t.Error("the selftestResponse does not set AuthID, so the caller is never told which account was targeted")
	} else if !strings.Contains(literal, "AuthID:    authID") && !strings.Contains(literal, "AuthID: authID") {
		t.Errorf("AuthID is not echoed verbatim from the request; literal was:\n%s", literal)
	}

	if !strings.Contains(literal, `Targeted:  authID != ""`) && !strings.Contains(literal, `Targeted: authID != ""`) {
		t.Errorf(`Targeted is not derived from 'authID != ""'; it must say whether the caller asked, not anything about the outcome. Literal was:`+"\n%s", literal)
	}
}

func TestSelftestTargetingIsActuallyApplied(t *testing.T) {
	body := managementFuncBody(t, "runSelftest")

	at := strings.Index(body, "HostModelExecutionRequest{")
	if at < 0 {
		t.Fatal("runSelftest builds no HostModelExecutionRequest; this test would pass vacuously")
	}
	end := strings.Index(body[at:], "\n\t}")
	if end < 0 {
		t.Fatal("could not find the end of the HostModelExecutionRequest literal")
	}
	literal := body[at : at+end]

	if !strings.Contains(literal, "AuthID:") {
		t.Error("the outbound HostModelExecutionRequest does not set AuthID, " +
			"so auth_id is validated and echoed but never applied: targeted:true would describe a request that was never targeted")
	}
}

func TestSelftestNeverClaimsHarvest(t *testing.T) {
	src, err := os.ReadFile("management.go")
	if err != nil {
		t.Fatalf("read management.go: %v", err)
	}
	text := string(src)

	const field = "Harvested"
	if !strings.Contains(text, field) {
		t.Fatalf("management.go has no %s field; the selftest contract requires one reported as false", field)
	}

	assignments := 0
	for idx := 0; ; {
		at := strings.Index(text[idx:], field)
		if at < 0 {
			break
		}
		at += idx
		idx = at + len(field)

		rest := strings.TrimLeft(text[idx:], " \t")

		var value string
		switch {
		case strings.HasPrefix(rest, ":") && !strings.HasPrefix(rest, ":="):
			value = strings.TrimLeft(rest[1:], " \t")
		case strings.HasPrefix(rest, "="):
			value = strings.TrimLeft(rest[1:], " \t")
		default:
			continue
		}
		assignments++
		if !strings.HasPrefix(value, "false") {
			line := 1 + strings.Count(text[:at], "\n")
			t.Errorf("management.go:%d assigns %s a computed value (%q); it must be the literal false, because the selftest structurally cannot harvest",
				line, field, truncateMgmtLog([]byte(value[:min(40, len(value))])))
		}
	}
	if assignments == 0 {
		t.Error("no assignment to Harvested found; this test would pass vacuously")
	}
}

func TestManagementUnknownPathReturns404(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	for _, path := range []string{
		"/v0/management/codex-turn-state/nope",
		"/v0/management/other-plugin/status",
		"/v0/management/codex-turn-state/status/extra",
		"/v0/management/codex-turn-state/buckets",
	} {
		t.Run(path, func(t *testing.T) {
			resp := driveManagement(t, http.MethodGet, path, nil)
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("unknown path %q returned %d, want 404", path, resp.StatusCode)
			}
		})
	}
}

func TestManagementRejectsWrongMethod(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", wallClock().Add(-time.Minute))
	before := observedCellCount()

	cases := []struct{ method, path string }{
		{http.MethodPost, mgmtStatusPath},
		{http.MethodDelete, mgmtStatusPath},
		{http.MethodGet, mgmtClearPath},
		{http.MethodGet, mgmtSelftestPath},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp := driveManagement(t, tc.method, tc.path, nil)
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				t.Errorf("%s %s returned %d, want 4xx", tc.method, tc.path, resp.StatusCode)
			}
		})
	}
	if observedCellCount() != before {
		t.Error("a wrong-method call still modified the tally")
	}
}

func TestManagementHandleSurvivesMalformedInput(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	for _, raw := range [][]byte{
		nil,
		[]byte(""),
		[]byte("{"),
		[]byte("[]"),
		[]byte(`{"Method":42}`),
		[]byte(`{"Path":null,"Method":null}`),
	} {

		if _, err := handleMethod(pluginabi.MethodManagementHandle, raw); err != nil {
			t.Logf("management.handle rejected %q: %v", truncateMgmtLog(raw), err)
		}
	}
}

func TestManagementCountersTrackDecisions(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, businessConfig(dir, false))
	resetHarvestState(t)

	issued := wallClock().Add(-time.Minute)
	seedPoolEntry(t, map[string]string{"__cflb": "cf", "__oailb": "lb"}, time.Now(), "")

	base := mustManagementStatus(t).Counters

	interceptAfter(t, request("codex-alpha.json", "gpt-5.5", fakeToken(312, issued)))
	interceptAfter(t, request("codex-beta.json", "gpt-5.6-sol", fakeToken(312, issued)))

	noAuth := request("", "gpt-5.5", fakeToken(312, issued))
	interceptAfter(t, noAuth)

	after := mustManagementStatus(t).Counters
	if got := after.Steer - base.Steer; got != 2 {
		t.Errorf("steer delta = %d, want 2", got)
	}
	if got := after.Skip - base.Skip; got != 1 {
		t.Errorf("skip delta = %d, want 1", got)
	}
}

func TestStatusReflectsConfiguredValues(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	status := mustManagementStatus(t)
	if status.Role != roleProbe {
		t.Errorf("role = %q, want %q", status.Role, roleProbe)
	}
	if !status.DryRun {
		t.Error("dry_run = false, want true")
	}
	if status.TTLSeconds != 3600 {
		t.Errorf("ttl_seconds = %d, want 3600", status.TTLSeconds)
	}
	if status.TemplateLength != 292 || status.ReplaceLength != 312 {
		t.Errorf("lengths = %d/%d, want 292/312", status.TemplateLength, status.ReplaceLength)
	}
	if len(status.Models) != 2 {
		t.Errorf("models = %v, want the 2 configured", status.Models)
	}
}

func TestStatusBucketLenIsALengthNotAValue(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	issued := wallClock().Add(-time.Minute)
	secret := seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", issued)

	resp := driveManagement(t, http.MethodGet, mgmtStatusPath, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status returned %d, want 200", resp.StatusCode)
	}
	if strings.Contains(string(resp.Body), secret[:40]) {
		t.Fatal("status leaked the token value alongside its length")
	}

	var status mgmtStatus
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	stored, ok := mgmtBucketByKey(status, "codex-alpha.json", "gpt-5.5")
	if !ok {
		t.Fatal("the seeded bucket is missing from status")
	}
	if stored.Len != 292 {
		t.Errorf("len = %d for a stored 292-character template, want 292", stored.Len)
	}

	missing, ok := mgmtBucketByKey(status, "codex-alpha.json", "gpt-5.6-sol")
	if !ok {
		t.Fatal("the unharvested target bucket is missing from status")
	}
	if missing.Len != 0 {
		t.Errorf("len = %d for a bucket that was never harvested, want 0", missing.Len)
	}
}

const (
	msgOverloaded = `host_call_failed: {"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later.","param":null},"sequence_number":2}`

	msgServerError = `host_call_failed: {"error":{"type":"server_error","code":"server_error","message":"An error occurred while processing your request.","param":null},"sequence_number":1}`
)

func TestUpstreamErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		message    string
		wantBody   bool
		wantCode   string
		wantType   string
		wantStatus int
		wantOKStat bool
	}{
		{

			name:     "overloaded, no status",
			message:  msgOverloaded,
			wantBody: true,
			wantCode: "server_is_overloaded",
			wantType: "service_unavailable_error",
		},
		{
			name:     "server_error, no status",
			message:  msgServerError,
			wantBody: true,
			wantCode: "server_error",
			wantType: "server_error",
		},
		{
			name:       "body and status together",
			message:    `host_call_failed: {"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"slow down"}} failed with status 429`,
			wantBody:   true,
			wantCode:   "rate_limit_exceeded",
			wantType:   "rate_limit_error",
			wantStatus: 429,
			wantOKStat: true,
		},
		{
			name:       "status only, no body",
			message:    "host_call_failed: request failed with status 502",
			wantBody:   false,
			wantStatus: 502,
			wantOKStat: true,
		},
		{

			name:    "transport failure",
			message: "host_call_failed: dial tcp 127.0.0.1:8317: connect: connection refused",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, okBody := upstreamErrorFrom(tc.message)
			if okBody != tc.wantBody {
				t.Fatalf("upstreamErrorFrom ok = %t, want %t", okBody, tc.wantBody)
			}
			if okBody {
				if got := strings.TrimSpace(body.Error.Code); got != tc.wantCode {
					t.Errorf("upstream_error_code = %q, want %q", got, tc.wantCode)
				}
				if got := strings.TrimSpace(body.Error.Type); got != tc.wantType {
					t.Errorf("upstream_error_type = %q, want %q", got, tc.wantType)
				}
			}

			status, okStatus := statusFromExecutionError(tc.message)
			if okStatus != tc.wantOKStat {
				t.Fatalf("statusFromExecutionError ok = %t, want %t", okStatus, tc.wantOKStat)
			}
			if okStatus && status != tc.wantStatus {
				t.Errorf("status = %d, want %d", status, tc.wantStatus)
			}

			if reached := okBody || okStatus; reached != (tc.wantBody || tc.wantOKStat) {
				t.Errorf("reached would be %t, want %t", reached, tc.wantBody || tc.wantOKStat)
			}
		})
	}
}

func TestUpstreamErrorFromRejectsUnrelatedJSON(t *testing.T) {
	for _, message := range []string{
		`host_call_failed: {"foo":"bar"}`,
		`host_call_failed: {}`,
		`host_call_failed: {"error":{}}`,
		`host_call_failed: {"error":{"type":"","code":"","message":""}}`,
		`host_call_failed: not json at all`,
		`host_call_failed: {"sequence_number":2}`,
	} {
		t.Run(message, func(t *testing.T) {
			if _, ok := upstreamErrorFrom(message); ok {
				t.Error("an unrelated JSON object was accepted as an upstream error body")
			}
		})
	}
}

func TestStatusFromExecutionErrorRequiresTheFullPhrase(t *testing.T) {
	rejected := []string{

		`host_call_failed: {"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Overloaded -- check status 503 page for updates."}}`,
		"host_call_failed: status 429",
		"host_call_failed: http status 500",
		"host_call_failed: failed with status",
		"host_call_failed: failed with status abc",

		"host_call_failed: failed with status 42",
		"host_call_failed: failed with status 900",
	}
	for _, message := range rejected {
		t.Run(message, func(t *testing.T) {
			if status, ok := statusFromExecutionError(message); ok {
				t.Errorf("recovered status %d from a message that carries none", status)
			}
		})
	}

	accepted := map[string]int{
		"host_call_failed: request failed with status 429": 429,
		"host_call_failed: request failed with status 500": 500,
		"host_call_failed: request failed with status 100": 100,
		"host_call_failed: request failed with status 599": 599,
	}
	for message, want := range accepted {
		t.Run(message, func(t *testing.T) {
			status, ok := statusFromExecutionError(message)
			if !ok {
				t.Fatalf("the documented phrasing was not recognised")
			}
			if status != want {
				t.Errorf("status = %d, want %d", status, want)
			}
		})
	}
}

func TestSelftestUpstreamFieldsHaveNoOmitempty(t *testing.T) {
	src, err := os.ReadFile("management.go")
	if err != nil {
		t.Fatalf("read management.go: %v", err)
	}
	for _, field := range []string{"upstream_error_code", "upstream_error_type"} {
		tag := `json:"` + field + `"`
		if !strings.Contains(string(src), tag) {
			t.Errorf("%s is not declared with a bare %s tag; an omitempty here would make the response shape vary", field, tag)
		}
	}
}

func harvestResponseHeaders(value string) http.Header {
	h := http.Header{}
	h.Set(testHeader, value)
	return h
}

func resetHarvestState(t *testing.T) {
	t.Helper()
	state.mu.Lock()
	state.cookies = make(map[string]*routeCookieEntry)
	state.cookiesDirty = false
	state.mu.Unlock()
	t.Cleanup(func() {
		state.mu.Lock()
		state.cookies = make(map[string]*routeCookieEntry)
		state.cookiesDirty = false
		state.mu.Unlock()
	})
}

func withAuthList(t *testing.T, accounts []codexAuth, err error) {
	t.Helper()
	codexAuthLister = func() ([]codexAuth, error) {
		return accounts, err
	}
	resetAuthCache()
	t.Cleanup(func() {
		codexAuthLister = listCodexAuths
		resetAuthCache()
	})
}

func enabledAccounts(names ...string) []codexAuth {
	out := make([]codexAuth, len(names))
	for i, name := range names {
		out[i] = codexAuth{AuthID: name, Enabled: true}
	}
	return out
}

var harvestNoAuthMeta = map[string]any{}

func TestStatusReportsDegradedAccountSource(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", wallClock().Add(-time.Minute))

	status := mustManagementStatus(t)
	if status.AccountsSource != "store" {
		t.Errorf("accounts_source = %q with no host API, want %q", status.AccountsSource, "store")
	}
	if strings.TrimSpace(status.AccountsError) == "" {
		t.Error("accounts_error is empty on the degraded path, so the fallback is silent")
	}
}

func TestStatusMatrixCoversEveryTarget(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfigModels(dir, "gpt-5.5", "gpt-5.6-sol"))

	issued := wallClock().Add(-time.Minute)

	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", issued)
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.6-sol", issued)
	seedMgmtBucket(t, dir, "codex-beta.json", "gpt-5.5", issued)

	status := mustManagementStatus(t)
	if status.TargetsTotal != len(status.Buckets) {
		t.Errorf("targets_total = %d but buckets has %d entries", status.TargetsTotal, len(status.Buckets))
	}
	if status.TargetsTotal != 4 {
		t.Errorf("targets_total = %d for 2 accounts x 2 models, want 4", status.TargetsTotal)
	}

	gap, ok := mgmtBucketByKey(status, "codex-beta.json", "gpt-5.6-sol")
	if !ok {
		t.Fatal("the never-harvested combination is missing from the matrix; the page would not show it as a gap")
	}
	if gap.Ready {
		t.Error("a never-harvested cell reports ready")
	}
	if gap.Len != 0 {
		t.Errorf("a never-harvested cell reports len = %d, want 0", gap.Len)
	}
	if gap.IssuedAt != "" || gap.ExpiresAt != "" {
		t.Errorf("a never-harvested cell carries timestamps: issued=%q expires=%q", gap.IssuedAt, gap.ExpiresAt)
	}
	if gap.SecondsLeft != 0 {
		t.Errorf("a never-harvested cell reports seconds_left = %d, want 0", gap.SecondsLeft)
	}

	mustConfigure(t, probeRoleConfigModels(dir, "gpt-5.5", "gpt-5.6-sol", "gpt-6-astra"))
	wider := mustManagementStatus(t)
	if wider.TargetsTotal != 6 {
		t.Errorf("targets_total = %d after adding a third model to 2 accounts, want 6", wider.TargetsTotal)
	}
}

func TestStatusBucketOrderIsStable(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfigModels(dir, "gpt-5.6-sol", "gpt-5.5"))

	issued := wallClock().Add(-time.Minute)

	seedMgmtBucket(t, dir, "codex-zulu.json", "gpt-5.6-sol", issued)
	seedMgmtBucket(t, dir, "codex-alpha.json", "gpt-5.5", issued)
	seedMgmtBucket(t, dir, "codex-mike.json", "gpt-5.6-sol", issued)

	first := mustManagementStatus(t)
	second := mustManagementStatus(t)

	if len(first.Buckets) != len(second.Buckets) {
		t.Fatalf("two consecutive calls returned %d and %d buckets", len(first.Buckets), len(second.Buckets))
	}
	for i := range first.Buckets {
		if first.Buckets[i].AuthID != second.Buckets[i].AuthID || first.Buckets[i].Model != second.Buckets[i].Model {
			t.Fatalf("order changed between calls at index %d: %s/%s then %s/%s",
				i, first.Buckets[i].AuthID, first.Buckets[i].Model,
				second.Buckets[i].AuthID, second.Buckets[i].Model)
		}
	}

	for i := 1; i < len(first.Buckets); i++ {
		prev, cur := first.Buckets[i-1], first.Buckets[i]
		if prev.AuthID > cur.AuthID || (prev.AuthID == cur.AuthID && prev.Model > cur.Model) {
			t.Errorf("buckets are not sorted by (auth_id, model): %s/%s precedes %s/%s",
				prev.AuthID, prev.Model, cur.AuthID, cur.Model)
		}
	}
}

func TestStatusEmptyStoreStillNamesItsSource(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeRoleConfig(dir))

	status := mustManagementStatus(t)
	if status.AccountsSource != "store" {
		t.Errorf("accounts_source = %q, want %q", status.AccountsSource, "store")
	}
	if strings.TrimSpace(status.AccountsError) == "" {
		t.Error("an empty matrix arrived with no accounts_error, so it reads as 'nothing to probe'")
	}
	if status.TargetsReady != 0 {
		t.Errorf("targets_ready = %d on an empty store, want 0", status.TargetsReady)
	}
	if status.TargetsTotal != len(status.Buckets) {
		t.Errorf("targets_total = %d but buckets has %d entries", status.TargetsTotal, len(status.Buckets))
	}
}

const (
	testProbeManagementKey = "mk-probe-management-never-show-me"
)

func probeConfigWithSecrets(dir string) string {
	return probeRoleConfig(dir) + fmt.Sprintf(`probe_accounts:
  - codex-a.json
probe_proxies:
  - %s
probe_management_key: %s
`, testProxyWithPW, testProbeManagementKey)
}

func TestStatusShowsProbeProxiesInTheClear(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeConfigWithSecrets(dir))

	status := mustManagementStatus(t)
	if status.ProbeProxyCount != 1 {
		t.Fatalf("probe_proxy_count = %d, want 1; the count stays alongside the list", status.ProbeProxyCount)
	}
	if len(status.ProbeProxies) != 1 || status.ProbeProxies[0] != testProxyWithPW {
		t.Fatalf("probe_proxies = %v, want the configured entry verbatim", status.ProbeProxies)
	}

	resp := driveManagement(t, http.MethodGet, mgmtResourcePath+"status", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous status returned %d, want 200 (body: %s)", resp.StatusCode, truncateMgmtLog(resp.Body))
	}
	var anonymous mgmtStatus
	if err := json.Unmarshal(resp.Body, &anonymous); err != nil {
		t.Fatalf("decode anonymous status: %v", err)
	}
	if len(anonymous.ProbeProxies) != 1 || anonymous.ProbeProxies[0] != testProxyWithPW {
		t.Fatalf("anonymous probe_proxies = %v, want the same verbatim entry", anonymous.ProbeProxies)
	}

	if strings.Contains(string(resp.Body), "probe_proxies_masked") {
		t.Error("status still carries probe_proxies_masked; the masked field was replaced, not supplemented")
	}
}

func TestProbeKeysAreNeverDisplayedOrLogged(t *testing.T) {
	dir := t.TempDir()

	logged := captureLog(t, func() { mustConfigure(t, probeConfigWithSecrets(dir)) })
	if strings.TrimSpace(logged) == "" {
		t.Fatal("configure logged nothing; the leak assertions below would prove nothing")
	}
	for _, secret := range []string{testProbeManagementKey} {
		if strings.Contains(logged, secret) {
			t.Error("the configure log line carried a probe key verbatim")
		}
	}

	for _, want := range []string{"probe_management_key=set"} {
		if !strings.Contains(logged, want) {
			t.Errorf("the configure log line does not report %q, so a missing key would be invisible: %s", want, logged)
		}
	}

	for name, path := range map[string]string{
		"management status": mgmtStatusPath,
		"anonymous status":  mgmtResourcePath + "status",
		"config":            mgmtConfigPath,
	} {
		resp := driveManagement(t, http.MethodGet, path, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %d, want 200 (body: %s)", name, resp.StatusCode, truncateMgmtLog(resp.Body))
		}
		if len(resp.Body) == 0 {
			t.Fatalf("%s body is empty; the assertions below would prove nothing", name)
		}
		body := string(resp.Body)
		for _, secret := range []string{testProbeManagementKey} {
			if strings.Contains(body, secret) {
				t.Errorf("%s leaked a probe key", name)
			}
		}

		for _, field := range []string{"probe_management_key"} {
			if strings.Contains(body, field) {
				t.Errorf("%s carries a %q field; these are never displayed, not even empty or masked", name, field)
			}
		}
	}
}

func TestProbeRunRoutesAreKeylessResourcesGuardedByConfirm(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, probeConfigWithSecrets(dir))

	reg := driveManagementRegister(t)
	if len(reg.Resources) == 0 {
		t.Fatal("no resources declared; this test would pass vacuously")
	}
	resources := make(map[string]mgmtRoute, len(reg.Resources))
	for _, res := range reg.Resources {
		resources[res.Path] = res
	}
	managementRoutes := make(map[string]bool, len(reg.Routes))
	for _, route := range reg.Routes {
		managementRoutes[route.Path] = true
	}

	for _, path := range []string{"/ops/probe/start", "/ops/probe/cancel"} {
		res, ok := resources[path]
		if !ok {
			t.Errorf("%s is not registered as a resource, so it would demand a management key the dashboard does not have", path)
			continue
		}

		if strings.TrimSpace(res.Menu) != "" {
			t.Errorf("%s declares Menu %q; it is fetched by the page, not navigated to", path, res.Menu)
		}
		if managementRoutes[path] {
			t.Errorf("%s is also a management route; a keyless action must live only on the unauthenticated prefix", path)
		}
	}

	for _, res := range reg.Resources {
		if strings.HasSuffix(res.Path, "/config") {
			t.Errorf("config route %q is registered as a resource; it must stay behind the management key", res.Path)
		}
	}

	for _, path := range []string{opsProbeStartPath, opsProbeCancelPath} {
		resp := driveResource(t, path, url.Values{})
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s without confirm=1 returned %d, want 400 (body: %s)", path, resp.StatusCode, truncateMgmtLog(resp.Body))
		}
	}

	if snapshot := probeRunSnapshot(); snapshot.Running {
		t.Error("a probe run is in flight after two requests that were refused for lack of confirm=1")
	}
}

type choicesCPAFile map[string]any

type choicesCPA struct {
	server *httptest.Server

	mu sync.Mutex

	status int
	files  []choicesCPAFile

	hold chan struct{}

	authSeen []string
}

func newChoicesCPA(t *testing.T, files ...choicesCPAFile) *choicesCPA {
	t.Helper()
	fake := &choicesCPA{status: http.StatusOK, files: files}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		hold, status, files := fake.hold, fake.status, fake.files
		fake.authSeen = append(fake.authSeen, r.Header.Get("Authorization"))
		fake.mu.Unlock()

		if hold != nil {
			<-hold
		}
		if r.URL.Path != probeRouteAuthFiles {
			http.Error(w, `{"error":"no such route"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != http.StatusOK {

			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"files": files})
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *choicesCPA) refuse(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
}

func (f *choicesCPA) stall(t *testing.T) {
	t.Helper()
	gate := make(chan struct{})
	f.mu.Lock()
	f.hold = gate
	f.mu.Unlock()
	t.Cleanup(func() { close(gate) })
}

func (f *choicesCPA) authHeaders() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.authSeen...)
}

const (
	choicesAuthPro  = "codex-11111111-pro@test.invalid-pro.json"
	choicesAuthPlus = "codex-22222222-plus@test.invalid-plus.json"

	choicesAuthBak = "codex-11111111-pro@test.invalid-pro.json.bak"

	choicesAuthOther = "gemini-other@test.invalid.json"
)

func choicesConfig(dir, baseURL, mgmtKey string, accounts, models []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "role: probe\nstore_dir: %q\nlog_decisions: false\ndry_run: true\n", dir)
	fmt.Fprintf(&b, "probe_base_url: %q\n", baseURL)
	fmt.Fprintf(&b, "probe_management_key: %q\n", mgmtKey)
	for _, block := range []struct {
		key    string
		values []string
	}{{"probe_accounts", accounts}, {"models", models}} {
		if len(block.values) == 0 {
			continue
		}
		fmt.Fprintf(&b, "%s:\n", block.key)
		for _, value := range block.values {
			fmt.Fprintf(&b, "  - %q\n", value)
		}
	}
	return b.String()
}

type mgmtChoiceAccount struct {
	Name     string `json:"name"`
	Label    string `json:"label"`
	Disabled bool   `json:"disabled"`
	Selected bool   `json:"selected"`
}

type mgmtChoiceModel struct {
	Name     string `json:"name"`
	Selected bool   `json:"selected"`
}

type mgmtChoices struct {
	Accounts []mgmtChoiceAccount `json:"accounts"`
	Models   []mgmtChoiceModel   `json:"models"`
	Error    string              `json:"error"`
}

func mustChoices(t *testing.T) (mgmtChoices, mgmtResponse) {
	t.Helper()
	resp := driveResource(t, opsChoicesPath, url.Values{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d, want 200 (body: %s)", opsChoicesPath, resp.StatusCode, truncateMgmtLog(resp.Body))
	}
	var out mgmtChoices
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("decode choices: %v (body: %s)", err, truncateMgmtLog(resp.Body))
	}
	return out, resp
}

func choiceAccountByName(choices mgmtChoices, name string) (mgmtChoiceAccount, bool) {
	for _, account := range choices.Accounts {
		if account.Name == name {
			return account, true
		}
	}
	return mgmtChoiceAccount{}, false
}

func TestChoicesIsAKeylessResourceNeedingNoConfirm(t *testing.T) {
	dir := t.TempDir()
	fake := newChoicesCPA(t, choicesCPAFile{"name": choicesAuthPro, "provider": "codex"})
	mustConfigure(t, choicesConfig(dir, fake.server.URL, "mk-choices", nil, []string{"gpt-5.5"}))

	reg := driveManagementRegister(t)
	if len(reg.Resources) == 0 {
		t.Fatal("no resources declared; this test would pass vacuously")
	}
	var declared *mgmtRoute
	for index, res := range reg.Resources {
		if res.Path == "/ops/choices" {
			declared = &reg.Resources[index]
		}
	}
	if declared == nil {
		t.Fatal("/ops/choices is not registered as a resource, so it would demand a management key the dashboard does not have")
	}

	if strings.TrimSpace(declared.Menu) != "" {
		t.Errorf("/ops/choices declares Menu %q; it is fetched by the page, not navigated to", declared.Menu)
	}
	for _, route := range reg.Routes {
		if route.Path == "/ops/choices" {
			t.Error("/ops/choices is also a management route; a keyless route must live only on the unauthenticated prefix")
		}
	}

	choices, _ := mustChoices(t)
	if len(choices.Models) == 0 {
		t.Error("choices returned no models on a keyless fetch; the checkboxes would render empty")
	}
}

func TestChoicesListsCPACredentialsAndMarksTheScope(t *testing.T) {
	dir := t.TempDir()
	const mgmtKey = "mk-choices-never-show-me"
	fake := newChoicesCPA(t,

		choicesCPAFile{"name": choicesAuthPlus, "provider": "codex", "disabled": true},
		choicesCPAFile{"name": choicesAuthPro, "provider": "codex", "disabled": false},
		choicesCPAFile{"name": choicesAuthBak, "provider": "codex"},
		choicesCPAFile{"name": choicesAuthOther, "provider": "gemini"},
	)
	mustConfigure(t, choicesConfig(dir, fake.server.URL, mgmtKey,
		[]string{choicesAuthPro},

		[]string{"gpt-5.5", "gpt-local-only"}))

	choices, resp := mustChoices(t)
	if choices.Error != "" {
		t.Fatalf("choices reported an error against a healthy CPA: %q", choices.Error)
	}

	gotNames := make([]string, 0, len(choices.Accounts))
	for _, account := range choices.Accounts {
		gotNames = append(gotNames, account.Name)
	}

	wantNames := []string{choicesAuthPro, choicesAuthPlus}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("accounts = %v, want exactly %v (a .bak copy or a non-Codex credential leaked into the menu)", gotNames, wantNames)
	}
	for index, want := range wantNames {
		if gotNames[index] != want {
			t.Fatalf("accounts = %v, want %v in that order", gotNames, wantNames)
		}
	}

	selected, _ := choiceAccountByName(choices, choicesAuthPro)
	if !selected.Selected {
		t.Error("the account in probe_accounts came back unselected; the page would render the saved scope as empty")
	}
	if selected.Disabled {
		t.Error("an enabled credential came back disabled")
	}
	unselected, _ := choiceAccountByName(choices, choicesAuthPlus)
	if unselected.Selected {
		t.Error("an account that is not in probe_accounts came back selected; ticking it was nobody's decision")
	}

	if !unselected.Disabled {
		t.Error("a credential CPA reports as disabled came back enabled")
	}

	wantModels := []struct {
		name     string
		selected bool
	}{
		{"gpt-5.5", true},
		{"gpt-5.6-sol", false},
		{"gpt-6-astra", false},
		{"gpt-local-only", true},
	}
	if len(choices.Models) != len(wantModels) {
		t.Fatalf("models = %+v, want %d entries (the union of the configured list and the fallback menu, de-duplicated)", choices.Models, len(wantModels))
	}
	for index, want := range wantModels {
		got := choices.Models[index]
		if got.Name != want.name {
			t.Fatalf("models[%d] = %q, want %q; the union must be sorted so the checkboxes do not reshuffle", index, got.Name, want.name)
		}
		if got.Selected != want.selected {
			t.Errorf("model %q selected=%v, want %v", got.Name, got.Selected, want.selected)
		}
	}

	body := string(resp.Body)
	if strings.Contains(body, mgmtKey) {
		t.Error("the choices body carries probe_management_key; this route answers without any key at all")
	}
	if strings.Contains(body, "probe_management_key\"") {
		t.Error("the choices body carries a probe_management_key field; these are never displayed, not even empty")
	}

	headers := fake.authHeaders()
	if len(headers) == 0 {
		t.Fatal("the fake CPA saw no request; the accounts above came from somewhere else")
	}
	if headers[0] != "Bearer "+mgmtKey {
		t.Errorf("CPA was called with Authorization %q, want the configured probe_management_key as a bearer", headers[0])
	}
}

func TestMaskAuthLabel(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "the normal codex-<hex>-<email>-<tier>.json shape",
			in:   "codex-620f5a42-user@test.invalid-pro.json",
			want: "620f5a42…pro",
		},
		{
			name: "no email in the name at all",
			in:   "codex-620f5a42-pro.json",
			want: "620f5a42…pro",
		},
		{

			name: "extra dashes around the email",
			in:   "codex-620f5a42--user@test.invalid--pro.json",
			want: "620f5a42…pro",
		},
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{

			name: "email in the final position",
			in:   "codex-620f5a42-user@test.invalid.json",
			want: "620f5a42",
		},
		{
			name: "nothing but an email",
			in:   "user@test.invalid",
			want: "…",
		},
	}
	for _, c := range cases {
		got := maskAuthLabel(c.in)
		if got != c.want {
			t.Errorf("%s: maskAuthLabel(%q) = %q, want %q", c.name, c.in, got, c.want)
		}

		if strings.Contains(got, "@") {
			t.Errorf("%s: maskAuthLabel(%q) = %q, which still carries an email address", c.name, c.in, got)
		}
	}
}

func TestChoicesDegradesWhenCredentialListUnavailable(t *testing.T) {
	t.Run("management key unset", func(t *testing.T) {
		dir := t.TempDir()
		fake := newChoicesCPA(t, choicesCPAFile{"name": choicesAuthPro, "provider": "codex"})
		mustConfigure(t, choicesConfig(dir, fake.server.URL, "", nil, []string{"gpt-5.5"}))

		choices, _ := mustChoices(t)
		if len(choices.Accounts) != 0 {
			t.Errorf("accounts = %+v, want [] when the credential list could not be fetched", choices.Accounts)
		}
		if !strings.Contains(choices.Error, "probe_management_key") {
			t.Errorf("error = %q; it must name the setting that is missing, or the operator has nothing to act on", choices.Error)
		}

		if len(choices.Models) != len(knownCodexModels) {
			t.Errorf("models = %+v, want the fallback menu; a credential fetch failure must not take the model list with it", choices.Models)
		}

		if headers := fake.authHeaders(); len(headers) != 0 {
			t.Errorf("CPA was called %d time(s) with no key configured; the refusal must be local", len(headers))
		}
	})

	t.Run("cpa rejects the key", func(t *testing.T) {
		dir := t.TempDir()
		const mgmtKey = "mk-choices-stale-never-show-me"
		fake := newChoicesCPA(t, choicesCPAFile{"name": choicesAuthPro, "provider": "codex"})
		fake.refuse(http.StatusUnauthorized)
		mustConfigure(t, choicesConfig(dir, fake.server.URL, mgmtKey, []string{choicesAuthPro}, []string{"gpt-5.5"}))

		choices, resp := mustChoices(t)
		if len(choices.Accounts) != 0 {
			t.Errorf("accounts = %+v, want [] after a 401", choices.Accounts)
		}
		if !strings.Contains(choices.Error, "401") {
			t.Errorf("error = %q; a 401 must be reported as one, because it means a wrong key rather than a wrong path", choices.Error)
		}
		if len(choices.Models) == 0 {
			t.Error("the model list went missing along with the accounts")
		}

		if strings.Contains(string(resp.Body), mgmtKey) {
			t.Error("the error body carries probe_management_key verbatim")
		}
	})
}

func TestChoicesDoesNotHangOnUnresponsiveCPA(t *testing.T) {
	dir := t.TempDir()
	fake := newChoicesCPA(t, choicesCPAFile{"name": choicesAuthPro, "provider": "codex"})
	fake.stall(t)
	mustConfigure(t, choicesConfig(dir, fake.server.URL, "mk-choices", nil, []string{"gpt-5.5"}))

	previous := choicesFetchTimeout
	choicesFetchTimeout = 100 * time.Millisecond
	t.Cleanup(func() { choicesFetchTimeout = previous })

	request, errMarshal := json.Marshal(map[string]any{
		"Method": http.MethodGet, "Path": opsChoicesPath,
		"Headers": http.Header{}, "Query": url.Values{}, "Body": nil,
	})
	if errMarshal != nil {
		t.Fatalf("marshal resource request: %v", errMarshal)
	}

	type outcome struct {
		raw []byte
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		raw, err := handleMethod(pluginabi.MethodManagementHandle, request)
		done <- outcome{raw: raw, err: err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("handleMethod(management.handle) GET %s: %v", opsChoicesPath, got.err)
		}
		var resp mgmtResponse
		if result := decodeMgmtEnvelope(t, got.raw); len(result) > 0 {
			if errUnmarshal := json.Unmarshal(result, &resp); errUnmarshal != nil {
				t.Fatalf("decode resource response: %v", errUnmarshal)
			}
		}
		if resp.StatusCode != 0 && resp.StatusCode != http.StatusOK {
			t.Fatalf("a timed-out fetch returned %d, want 200 with the reason in the body", resp.StatusCode)
		}
		var choices mgmtChoices
		if err := json.Unmarshal(resp.Body, &choices); err != nil {
			t.Fatalf("decode choices: %v (body: %s)", err, truncateMgmtLog(resp.Body))
		}
		if choices.Error == "" {
			t.Error("a timed-out fetch reported no error; the page would render an empty account list as if CPA held none")
		}
		if len(choices.Models) == 0 {
			t.Error("the model list went missing on a timeout, though it needs no CPA call")
		}
	case <-time.After(10 * time.Second):

		t.Fatal("/ops/choices did not return against an unresponsive CPA")
	}
}
