package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testProxySecret = "s3cr3t-never-log-me"
	testProxyWithPW = "socks5h://prober:" + testProxySecret + "@exit.invalid:1080"
)

func TestMaskProxyURLNeverEchoesUserinfo(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"userinfo is replaced wholesale", testProxyWithPW, "socks5h://***@exit.invalid:1080"},
		{"user without password still masked", "http://user@exit.invalid:8080", "http://***@exit.invalid:8080"},
		{"no userinfo passes through", "socks5://exit.invalid:1080", "socks5://exit.invalid:1080"},
		{"empty stays empty", "", ""},

		{"unparsable is not echoed", "://" + testProxySecret, "<unparsable proxy url>"},
		{"schemeless is not echoed", "exit.invalid:1080", "<unparsable proxy url>"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := maskProxyURL(tc.in)
			if got != tc.want {
				t.Fatalf("maskProxyURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if strings.Contains(got, testProxySecret) {
				t.Fatalf("maskProxyURL leaked the password: %q", got)
			}
		})
	}
}

func TestMaskProxyURLsKeepsPositions(t *testing.T) {

	in := []string{testProxyWithPW, "http://exit2.invalid:8080", "://broken"}
	got := maskProxyURLs(in)
	if len(got) != len(in) {
		t.Fatalf("maskProxyURLs returned %d entries for %d inputs", len(got), len(in))
	}
	if got[1] != "http://exit2.invalid:8080" {
		t.Fatalf("entry without userinfo was altered: %q", got[1])
	}
	for i, value := range got {
		if strings.Contains(value, testProxySecret) {
			t.Fatalf("masked entry %d leaked the password: %q", i, value)
		}
	}
}

func TestNormaliseProbeScopeDropsAndReportsBadEntries(t *testing.T) {
	accounts, models, proxies, _, problems := normaliseProbeScope(
		[]string{"codex-a.json", "  codex-b.json  ", "", "notcodex.json", "codex-../escape.json"},
		[]string{"gpt-5.6-sol", " gpt-6-astra ", "", "gpt 5.5"},
		[]string{testProxyWithPW, "", "ftp://exit.invalid:21", "://broken"},
		nil,
	)

	wantAccounts := []string{"codex-a.json", "codex-b.json"}
	if !equalStrings(accounts, wantAccounts) {
		t.Fatalf("accounts = %v, want %v", accounts, wantAccounts)
	}
	wantModels := []string{"gpt-5.6-sol", "gpt-6-astra"}
	if !equalStrings(models, wantModels) {
		t.Fatalf("models = %v, want %v", models, wantModels)
	}
	if len(proxies) != 1 || proxies[0] != testProxyWithPW {
		t.Fatalf("proxies = %v, want just the one valid entry", maskProxyURLs(proxies))
	}

	if len(problems) != 5 {
		t.Fatalf("expected 5 complaints, got %d: %v", len(problems), problems)
	}
}

func TestProbeScopeProblemsNeverContainAPassword(t *testing.T) {

	_, _, _, _, problems := normaliseProbeScope(
		nil, nil,
		[]string{
			"ftp://prober:" + testProxySecret + "@exit.invalid:21",
			"://" + testProxySecret,
			"gopher://prober:" + testProxySecret + "@exit.invalid:70",
		},

		[]string{"gopher://prober:" + testProxySecret + "@rotate.invalid:70"},
	)
	if len(problems) == 0 {
		t.Fatal("expected complaints about three bad proxies, got none")
	}
	for _, problem := range problems {
		if strings.Contains(problem, testProxySecret) {
			t.Fatalf("a config error leaked the password: %q", problem)
		}
	}
}

func TestConfigureNeverFailsOnBadProbeScope(t *testing.T) {

	dir := t.TempDir()
	cfg := fmt.Sprintf(`role: business
store_dir: %q
dry_run: false
log_decisions: false
probe_accounts:
  - not-a-codex-file.txt
probe_proxies:
  - ftp://exit.invalid:21
`, dir)
	if err := configureYAML(t, cfg); err != nil {
		t.Fatalf("configure rejected a config whose only fault was probe scope: %v", err)
	}

	state.mu.Lock()
	gotAccounts := len(state.config.ProbeAccounts)
	gotProxies := len(state.config.ProbeProxies)
	gotErrors := len(state.configErrors)
	state.mu.Unlock()

	if gotAccounts != 0 || gotProxies != 0 {
		t.Fatalf("invalid entries were kept: %d accounts, %d proxies", gotAccounts, gotProxies)
	}
	if gotErrors != 2 {
		t.Fatalf("expected 2 recorded config errors, got %d", gotErrors)
	}
}

func TestProbeScopeChangeKeepsThePool(t *testing.T) {

	base := defaultConfig()
	base.Role = roleBusiness
	base.StoreDir = "/data/turn-state-store"
	base.Models = []string{"gpt-5.6-sol"}

	for _, tc := range []struct {
		name   string
		mutate func(*pluginConfig)
	}{
		{"probe_accounts changed", func(c *pluginConfig) { c.ProbeAccounts = []string{"codex-a.json"} }},
		{"probe_proxies changed", func(c *pluginConfig) { c.ProbeProxies = []string{testProxyWithPW} }},
		{"models changed", func(c *pluginConfig) { c.Models = []string{"gpt-6-astra"} }},
		{"ttl_seconds changed", func(c *pluginConfig) { c.TTLSeconds = c.TTLSeconds / 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := base
			tc.mutate(&next)
			if poolInvalidatedBy(base, next) {
				t.Fatal("a probe-scope edit cleared the pool")
			}
		})
	}

	moved := base
	moved.StoreDir = base.StoreDir + "-moved"
	if !poolInvalidatedBy(base, moved) {
		t.Fatal("a store_dir change no longer clears the pool; the control case is broken")
	}
}

func TestBusinessSteersForAnAccountOutsideTheProbeScope(t *testing.T) {

	dir := t.TempDir()
	const (
		served = "codex-served.json"
		scoped = "codex-scoped.json"
		model  = "gpt-5.6-sol"
	)

	cfg := fmt.Sprintf(`role: business
store_dir: %q
template_length: 292
replace_length: 312
ttl_seconds: 3600
dry_run: false
log_decisions: false
probe_accounts:
  - %s
probe_proxies:
  - %s
`, dir, scoped, testProxyWithPW)
	mustConfigure(t, cfg)
	seedPoolEntry(t, map[string]string{"__cflb": "cf", "__oailb": "lb"}, time.Now(), "")

	resp := interceptAfter(t, request(served, model, fakeTokenSeed(312, wallClock(), 0x77)))
	cookie := resp.Headers.Get("Cookie")
	if !strings.Contains(cookie, "__cflb=cf") {
		t.Fatalf("probe_accounts filtered the business path: an out-of-scope Codex account was not steered (cookie %q)", cookie)
	}
}

func TestProbeScopeNeverCreatesABucket(t *testing.T) {

	dir := t.TempDir()
	const (
		scoped = "codex-scoped.json"
		model  = "gpt-5.6-sol"
	)
	cfg := fmt.Sprintf(`role: business
store_dir: %q
dry_run: false
log_decisions: false
probe_accounts:
  - %s
probe_proxies:
  - %s
`, dir, scoped, testProxyWithPW)
	mustConfigure(t, cfg)

	state.mu.Lock()
	before := len(state.cookies)
	state.mu.Unlock()
	resp := interceptAfter(t, request(scoped, model, fakeTokenSeed(312, wallClock(), 0x21)))
	if cookie := resp.Headers.Get("Cookie"); cookie != "" {
		t.Fatalf("a request was steered with an empty pool (cookie %q)", cookie)
	}
	state.mu.Lock()
	after := len(state.cookies)
	state.mu.Unlock()
	if after != before {
		t.Fatalf("the business path minted into the pool: %d -> %d", before, after)
	}
}

const opsScopePath = mgmtResourcePath + "ops/scope"

func scopeConfig(t *testing.T, dir string) string {
	t.Helper()
	return fmt.Sprintf(`role: probe
store_dir: %q
dry_run: true
log_decisions: false
probe_accounts:
  - codex-a.json
models:
  - gpt-5.6-sol
probe_proxies:
  - %s
`, dir, testProxyWithPW)
}

func TestScopeSaveRequiresConfirm(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, url.Values{"fields": {"models"}, "model": {"gpt-5.5"}})
	if resp.StatusCode == http.StatusOK {
		t.Fatal("the scope saved without confirm=1; a bare navigation or a prefetch could rewrite the probe scope")
	}
	if _, errStat := os.Stat(filepath.Join(dir, scopeFileName)); !os.IsNotExist(errStat) {
		t.Fatal("a scope file was written despite the request being rejected")
	}
}

func TestScopeSaveRequiresFields(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, confirmed(nil))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 when fields is missing", resp.StatusCode)
	}
	state.mu.Lock()
	proxies := len(state.config.ProbeProxies)
	state.mu.Unlock()
	if proxies != 1 {
		t.Fatalf("the proxy list was touched by a rejected save: %d entries left", proxies)
	}
}

func TestScopeSaveWritesFileAndAppliesLive(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, confirmed(url.Values{
		"fields":  {"accounts,models"},
		"account": {"codex-x.json", "codex-y.json"},
		"model":   {"gpt-5.6-sol", "gpt-6-astra"},
	}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200. body: %s", resp.StatusCode, resp.Body)
	}

	var out struct {
		Saved         bool     `json:"saved"`
		ProbeAccounts []string `json:"probe_accounts"`
		Models        []string `json:"models"`
		TargetsTotal  int      `json:"targets_total"`
	}
	if errUnmarshal := json.Unmarshal(resp.Body, &out); errUnmarshal != nil {
		t.Fatalf("decode save response: %v", errUnmarshal)
	}
	if !out.Saved || out.TargetsTotal != 4 {
		t.Fatalf("saved=%t targets_total=%d, want true and 4 (2 accounts x 2 models)", out.Saved, out.TargetsTotal)
	}

	state.mu.Lock()
	liveAccounts := append([]string(nil), state.config.ProbeAccounts...)
	state.mu.Unlock()
	if !equalStrings(liveAccounts, []string{"codex-x.json", "codex-y.json"}) {
		t.Fatalf("in-memory config not updated: %v", liveAccounts)
	}

	saved, errLoad := loadProbeScope(dir)
	if errLoad != nil || saved == nil {
		t.Fatalf("scope file not written: %v", errLoad)
	}
	if !equalStrings(saved.Models, []string{"gpt-5.6-sol", "gpt-6-astra"}) {
		t.Fatalf("scope file models = %v", saved.Models)
	}
}

func TestScopeSaveOnlyReplacesNamedFields(t *testing.T) {

	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, confirmed(url.Values{
		"fields": {"models"},
		"model":  {"gpt-5.5"},
	}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200. body: %s", resp.StatusCode, resp.Body)
	}

	saved, errLoad := loadProbeScope(dir)
	if errLoad != nil || saved == nil {
		t.Fatalf("scope file not written: %v", errLoad)
	}
	if len(saved.Proxies) != 1 || saved.Proxies[0] != testProxyWithPW {
		t.Fatalf("the proxy list did not survive a models-only save: %v", maskProxyURLs(saved.Proxies))
	}
	if !equalStrings(saved.Accounts, []string{"codex-a.json"}) {
		t.Fatalf("the account list did not survive a models-only save: %v", saved.Accounts)
	}

	resp = driveResource(t, opsScopePath, confirmed(url.Values{"fields": {"proxies"}}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("explicit clear: status = %d", resp.StatusCode)
	}
	saved, _ = loadProbeScope(dir)
	if saved == nil || len(saved.Proxies) != 0 {
		t.Fatal("fields=proxies with no proxy params should clear the list")
	}
}

func TestSavedScopeOverridesConfigYAML(t *testing.T) {

	dir := t.TempDir()
	if errWrite := writeProbeScope(dir, probeScope{
		Accounts:  []string{"codex-saved.json"},
		Models:    []string{"gpt-6-astra"},
		UpdatedAt: "2026-09-18T00:00:00Z",
	}); errWrite != nil {
		t.Fatalf("write scope: %v", errWrite)
	}

	mustConfigure(t, scopeConfig(t, dir))

	state.mu.Lock()
	accounts := append([]string(nil), state.config.ProbeAccounts...)
	models := append([]string(nil), state.config.Models...)
	proxies := len(state.config.ProbeProxies)
	state.mu.Unlock()

	if !equalStrings(accounts, []string{"codex-saved.json"}) {
		t.Fatalf("config.yaml won over the saved scope: %v", accounts)
	}
	if !equalStrings(models, []string{"gpt-6-astra"}) {
		t.Fatalf("models came from config.yaml, not the saved scope: %v", models)
	}

	if proxies != 0 {
		t.Fatalf("proxies leaked in from config.yaml: %d", proxies)
	}
}

func TestScopeSaveKeepsThePool(t *testing.T) {
	dir := t.TempDir()
	mustConfigure(t, fmt.Sprintf(`role: business
store_dir: %q
dry_run: false
log_decisions: false
probe_accounts:
  - codex-a.json
models:
  - gpt-5.6-sol
`, dir))
	seedPoolEntry(t, map[string]string{"__cflb": "cf", "__oailb": "lb"}, time.Now(), "")

	interceptAfter(t, request("codex-a.json", "gpt-5.6-sol", fakeTokenSeed(312, wallClock(), 0x11)))

	resp := driveResource(t, opsScopePath, confirmed(url.Values{
		"fields":  {"accounts"},
		"account": {"codex-b.json"},
	}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	out := interceptAfter(t, request("codex-a.json", "gpt-5.6-sol", fakeTokenSeed(312, wallClock(), 0x22)))
	if cookie := out.Headers.Get("Cookie"); !strings.Contains(cookie, "__cflb=cf") {
		t.Fatalf("the pair was lost when the scope was saved (cookie: %q)", cookie)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestScopeSaveRoundTripsTheRotatingPool(t *testing.T) {

	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, confirmed(url.Values{
		"fields":         {"rotating"},
		"rotating_proxy": {"socks5://gw:user@test.invalid:1080", "http://gw2.invalid:8080"},
	}))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", resp.StatusCode, resp.Body)
	}

	state.mu.Lock()
	rotating := append([]string(nil), state.config.ProbeProxiesRotating...)
	static := append([]string(nil), state.config.ProbeProxies...)
	state.mu.Unlock()

	if len(rotating) != 2 {
		t.Fatalf("rotating pool = %v, want the two saved entries", len(rotating))
	}

	if len(static) != 1 {
		t.Fatalf("static pool = %d entries, want 1 -- saving one pool rewrote the other", len(static))
	}

	mustConfigure(t, scopeConfig(t, dir))
	state.mu.Lock()
	reloaded := len(state.config.ProbeProxiesRotating)
	state.mu.Unlock()
	if reloaded != 2 {
		t.Fatalf("rotating pool after reload = %d, want 2; it was not persisted", reloaded)
	}
}

func TestScopeSaveRejectsAnUnknownField(t *testing.T) {

	dir := t.TempDir()
	mustConfigure(t, scopeConfig(t, dir))

	resp := driveResource(t, opsScopePath, confirmed(url.Values{"fields": {"rotaing"}}))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a misspelled field", resp.StatusCode)
	}
}

func TestRotatingPoolComplaintsAreMaskedToo(t *testing.T) {

	_, _, _, _, problems := normaliseProbeScope(nil, nil, nil,
		[]string{"gopher://gw:" + testProxySecret + "@rotate.invalid:70"})
	if len(problems) == 0 {
		t.Fatal("an unsupported scheme in the rotating pool produced no complaint")
	}
	for _, problem := range problems {
		if strings.Contains(problem, testProxySecret) {
			t.Fatalf("a rotating-pool complaint carried the password: %s", problem)
		}
		if !strings.Contains(problem, "probe_proxies_rotating") {
			t.Fatalf("complaint does not name which pool it came from: %s", problem)
		}
	}
}
