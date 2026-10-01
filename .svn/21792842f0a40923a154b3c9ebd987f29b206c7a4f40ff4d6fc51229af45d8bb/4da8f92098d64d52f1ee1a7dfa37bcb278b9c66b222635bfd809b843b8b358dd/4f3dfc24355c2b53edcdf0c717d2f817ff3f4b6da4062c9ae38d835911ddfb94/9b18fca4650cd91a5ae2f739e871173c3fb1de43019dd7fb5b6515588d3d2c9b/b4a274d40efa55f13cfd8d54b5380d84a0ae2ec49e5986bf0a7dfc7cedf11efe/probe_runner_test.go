package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeCredSeed struct {
	name      string
	accountID string
	proxyURL  string
	disabled  bool
	exp       time.Time

	noToken bool
}

type fakeCPA struct {
	mu     sync.Mutex
	server *httptest.Server
	seeds  map[string]fakeCredSeed
}

func encodeJWT(exp time.Time, accountID string) string {
	claims := map[string]any{
		"exp":                         exp.Unix(),
		"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": accountID},
	}
	raw, _ := json.Marshal(claims)
	return "e30." + base64.RawURLEncoding.EncodeToString(raw) + ".sig"
}

func newFakeCPA(t *testing.T, seeds ...fakeCredSeed) *fakeCPA {
	t.Helper()
	fake := &fakeCPA{seeds: make(map[string]fakeCredSeed, len(seeds))}
	for _, seed := range seeds {
		if seed.exp.IsZero() {
			seed.exp = time.Now().Add(72 * time.Hour)
		}
		fake.seeds[seed.name] = seed
	}
	fake.server = httptest.NewServer(fake)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeCPA) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == probeRouteAuthFiles:
		f.writeJSON(w, map[string]any{"files": f.fileList()})
	case r.Method == http.MethodGet && r.URL.Path == probeRouteAuthDownload:
		f.serveDownload(w, r)
	default:

		http.Error(w, `{"error":"the offline harvester must not call this route"}`, http.StatusMethodNotAllowed)
	}
}

func (f *fakeCPA) fileList() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, 0, len(f.seeds))
	for _, seed := range f.seeds {
		out = append(out, map[string]any{
			"name":     seed.name,
			"provider": "codex",
			"disabled": seed.disabled,
		})
	}
	return out
}

func (f *fakeCPA) serveDownload(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	f.mu.Lock()
	seed, found := f.seeds[name]
	f.mu.Unlock()
	if !found {
		http.Error(w, `{"error":"no such credential"}`, http.StatusNotFound)
		return
	}
	blob := map[string]any{
		"account_id":    seed.accountID,
		"proxy_url":     seed.proxyURL,
		"type":          "codex",
		"disabled":      seed.disabled,
		"email":         "user@test.invalid",
		"refresh_token": "refresh-must-never-be-used",
	}
	if !seed.noToken {
		blob["access_token"] = encodeJWT(seed.exp, seed.accountID)
	}
	f.writeJSON(w, blob)
}

func (f *fakeCPA) writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

type upstreamCall struct {
	model         string
	authorization string
	accountID     string
	sessionID     string
	sentTurnState bool

	cookie string
}

type fakeUpstream struct {
	mu     sync.Mutex
	server *httptest.Server
	calls  []upstreamCall

	status int
	tsLen  int

	tsLenSeq []int

	setCookies []string

	hold     time.Duration
	inFlight int
	peak     int
}

func (u *fakeUpstream) peakInFlight() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.peak
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()
	upstream := &fakeUpstream{status: http.StatusOK, tsLen: 292}
	upstream.server = httptest.NewServer(upstream)
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Model string `json:"model"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	u.mu.Lock()
	u.inFlight++
	if u.inFlight > u.peak {
		u.peak = u.inFlight
	}
	hold := u.hold
	u.mu.Unlock()
	if hold > 0 {
		time.Sleep(hold)
	}
	defer func() {
		u.mu.Lock()
		u.inFlight--
		u.mu.Unlock()
	}()

	u.mu.Lock()
	index := len(u.calls)
	u.calls = append(u.calls, upstreamCall{
		model:         body.Model,
		authorization: r.Header.Get("Authorization"),
		accountID:     r.Header.Get("Chatgpt-Account-Id"),
		sessionID:     r.Header.Get("Session-Id"),
		sentTurnState: r.Header.Get(turnStateHeader) != "",
		cookie:        r.Header.Get("Cookie"),
	})
	status, tsLen := u.status, u.tsLen
	setCookies := u.setCookies

	if len(u.tsLenSeq) > 0 {
		if index < len(u.tsLenSeq) {
			tsLen = u.tsLenSeq[index]
		} else {
			tsLen = u.tsLenSeq[len(u.tsLenSeq)-1]
		}
	}
	u.mu.Unlock()

	if status == http.StatusOK && tsLen >= 6 {
		w.Header().Set(turnStateHeader, "gAAAAA"+strings.Repeat("x", tsLen-6))
	}
	for _, line := range setCookies {
		w.Header().Add("Set-Cookie", line)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
}

func (u *fakeUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.calls)
}

func (u *fakeUpstream) snapshot() []upstreamCall {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]upstreamCall(nil), u.calls...)
}

const (
	probeTestAccount = "codex-11111111-user@test.invalid-pro.json"
	probeTestOther   = "codex-22222222-other@test.invalid-pro.json"
	probeTestModel   = "gpt-runner-1"
)

type probeConfigOptions struct {
	dir      string
	baseURL  string
	role     string
	mgmtKey  string
	accounts []string
	models   []string
	proxies  []string

	templateLen int
	replaceLen  int
	ttlSeconds  int
}

func probeTestOptions(dir, baseURL string) probeConfigOptions {
	return probeConfigOptions{
		dir:         dir,
		baseURL:     baseURL,
		role:        roleProbe,
		mgmtKey:     "test-mgmt-key",
		accounts:    []string{probeTestAccount},
		models:      []string{probeTestModel},
		templateLen: 292,
		replaceLen:  312,
		ttlSeconds:  3600,
	}
}

func probeTestConfig(opts probeConfigOptions) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "role: %s\nstore_dir: %q\nlog_decisions: false\n", opts.role, opts.dir)
	fmt.Fprintf(&builder, "template_length: %d\nreplace_length: %d\nttl_seconds: %d\n", opts.templateLen, opts.replaceLen, opts.ttlSeconds)
	fmt.Fprintf(&builder, "probe_base_url: %q\n", opts.baseURL)
	fmt.Fprintf(&builder, "probe_management_key: %q\n", opts.mgmtKey)
	for _, block := range []struct {
		key    string
		values []string
	}{
		{"probe_accounts", opts.accounts},
		{"models", opts.models},
		{"probe_proxies", opts.proxies},
	} {
		if len(block.values) == 0 {
			continue
		}
		fmt.Fprintf(&builder, "%s:\n", block.key)
		for _, value := range block.values {
			fmt.Fprintf(&builder, "  - %q\n", value)
		}
	}
	return builder.String()
}

func resetProbeRunner(t *testing.T) {
	t.Helper()
	clear := func() {
		probeRunner.mu.Lock()
		probeRunner.run = probeRunState{}
		probeRunner.cancel = nil
		probeRunner.mu.Unlock()
		probeActive.mu.Lock()
		probeActive.set = make(map[string]bool)
		probeActive.mu.Unlock()

		probeCooldown.mu.Lock()
		probeCooldown.until = make(map[string]time.Time)
		probeCooldown.mu.Unlock()

		probeAccountRest.mu.Lock()
		probeAccountRest.until = make(map[string]time.Time)
		probeAccountRest.mu.Unlock()
		state.mu.Lock()
		state.cookies = make(map[string]*routeCookieEntry)
		state.cookiesDirty = false
		state.cookiesFlushed = time.Time{}
		state.mu.Unlock()
	}
	t.Cleanup(func() {
		probeRunCancel()
		waitForProbeRun(t)
		clear()
	})
	clear()
}

func setUpstream(t *testing.T, rawURL string) {
	t.Helper()
	previous := probeUpstreamURL
	probeUpstreamURL = rawURL
	t.Cleanup(func() { probeUpstreamURL = previous })
}

func fastRenew(t *testing.T, interval, threshold, cooldown time.Duration) {
	t.Helper()
	prevInterval, prevThreshold, prevCooldown := probeRenewInterval, probeRenewThreshold, probeExitCooldown
	probeRenewInterval, probeRenewThreshold, probeExitCooldown = interval, threshold, cooldown
	t.Cleanup(func() {
		probeRenewInterval, probeRenewThreshold, probeExitCooldown = prevInterval, prevThreshold, prevCooldown
	})
}

func waitForProbeRun(t *testing.T) probeRunState {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		run := probeRunSnapshot()
		if !run.Running {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe run did not finish; last state %+v", run)
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitForInitialFill(t *testing.T) {
	t.Helper()
	waitUntil(t, "initial fill to finish", func() bool {
		run := probeRunSnapshot()
		return run.Total > 0 && run.Done >= run.Total
	})
}

func poolEntryCount() int {
	state.mu.Lock()
	defer state.mu.Unlock()
	return len(state.cookies)
}

func startProbeRun(t *testing.T) {
	t.Helper()
	if errStart := probeRunStart(); errStart != nil {
		t.Fatalf("probeRunStart refused a complete config: %v", errStart)
	}
	t.Cleanup(func() {
		probeRunCancel()
		waitForProbeRun(t)
	})
}

func TestProbeRunStartRefusesIncompleteConfig(t *testing.T) {

	tests := []struct {
		name   string
		narrow func(opts *probeConfigOptions)
		want   string
	}{
		{"no accounts", func(o *probeConfigOptions) { o.accounts = nil }, "probe_accounts"},
		{"no models", func(o *probeConfigOptions) { o.models = nil }, "models"},
		{"no management key", func(o *probeConfigOptions) { o.mgmtKey = "" }, "probe_management_key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetProbeRunner(t)
			dir := t.TempDir()
			opts := probeTestOptions(dir, "http://127.0.0.1:1")
			tc.narrow(&opts)
			mustConfigure(t, probeTestConfig(opts))

			errStart := probeRunStart()
			if errStart == nil {
				t.Fatalf("probeRunStart accepted a config with %s missing", tc.want)
			}
			if !strings.Contains(errStart.Error(), tc.want) {
				t.Fatalf("the refusal does not name %s: %v", tc.want, errStart)
			}
			if probeRunSnapshot().Running {
				t.Fatal("a refused start left the runner marked as running")
			}
		})
	}
}

func TestProbeRunStartRefusesEmptyStoreDir(t *testing.T) {

	resetProbeRunner(t)
	opts := probeTestOptions("", "http://127.0.0.1:1")
	state.mu.Lock()
	state.config = pluginConfig{
		Role:               roleProbe,
		ProbeManagementKey: opts.mgmtKey,
		ProbeAccounts:      opts.accounts,
		Models:             opts.models,
		TemplateLength:     292,
		ReplaceLength:      312,
	}
	state.mu.Unlock()

	errStart := probeRunStart()
	if errStart == nil || !strings.Contains(errStart.Error(), "store_dir") {
		t.Fatalf("probeRunStart did not refuse an empty store_dir: %v", errStart)
	}
}

func TestProbeRunStartIsSingleFlight(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a"})
	upstream := newFakeUpstream(t)
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))

	startProbeRun(t)
	errSecond := probeRunStart()
	if errSecond == nil {
		t.Fatal("a second start was accepted while a run was already going")
	}
	if !strings.Contains(errSecond.Error(), "already running") {
		t.Fatalf("the second refusal gave an unexpected reason: %v", errSecond)
	}
}

func TestProbePoolsTheMintedPair(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a"})
	upstream := newFakeUpstream(t)
	upstream.tsLen = 292
	upstream.setCookies = []string{"__cflb=cf-minted; Path=/", "__oailb=lb-minted; Path=/"}
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))
	startProbeRun(t)
	waitForInitialFill(t)

	if poolEntryCount() == 0 {
		t.Fatalf("no pair pooled for the account %q", probeTestAccount)
	}

	calls := upstream.snapshot()
	if len(calls) != 1 {
		t.Fatalf("upstream received %d calls, want exactly 1", len(calls))
	}
	call := calls[0]
	if !strings.HasPrefix(call.authorization, "Bearer ") {
		t.Fatalf("upstream call was not authorised: %q", call.authorization)
	}
	if call.accountID != "acct-a" {
		t.Fatalf("upstream call carried account id %q, want acct-a", call.accountID)
	}
	if call.sessionID == "" {
		t.Fatal("upstream call carried no Session-Id")
	}
	if call.sentTurnState {
		t.Fatal("the probe sent a turn-state upstream, which stops a fresh one being minted")
	}
	if call.cookie != "" {
		t.Fatalf("the probe sent cookies on a mint call -- a carried pair pins the node and the edge mints nothing: %q", call.cookie)
	}
}

func TestProbeSkipsThrottled312(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a"})
	upstream := newFakeUpstream(t)
	upstream.tsLen = 312
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))
	startProbeRun(t)
	waitForInitialFill(t)

	if poolEntryCount() != 0 {
		t.Fatal("a 312 that set no cookies still pooled an entry")
	}
	joined := strings.Join(probeRunSnapshot().Lines, "\n")
	if !strings.Contains(joined, "degraded") {
		t.Fatalf("the transcript does not explain the 312 was skipped: %s", joined)
	}
}

func TestProbeSkipsExpiredTokenWithoutRefreshing(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{
		name:      probeTestAccount,
		accountID: "acct-a",
		exp:       time.Now().Add(-1 * time.Minute),
	})
	upstream := newFakeUpstream(t)
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))
	startProbeRun(t)
	run := waitForProbeRun(t)

	if run.Error == "" {
		t.Fatal("a run with only an expired credential did not fail")
	}
	if upstream.count() != 0 {
		t.Fatalf("upstream was called %d times on an expired token; it must be skipped", upstream.count())
	}
	if !strings.Contains(strings.Join(run.Lines, "\n"), "expired") {
		t.Fatalf("the transcript does not say the token was skipped as expired: %v", run.Lines)
	}
}

func TestProbeUnreadableTokenIsSkipped(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a", noToken: true})
	upstream := newFakeUpstream(t)
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))
	startProbeRun(t)
	run := waitForProbeRun(t)
	if run.Error == "" {
		t.Fatal("a run whose only credential had no token did not fail")
	}
	if upstream.count() != 0 {
		t.Fatal("the upstream was called for a credential with no token")
	}
}

func TestProbeExitsAssignInOrderWithFallback(t *testing.T) {

	got := probeExits([]string{"p0", "p1", "p2"}, 0)
	if fmt.Sprint(got) != fmt.Sprint([]string{"p0", "p1", "p2"}) {
		t.Fatalf("account 0 order = %v, want p0,p1,p2", got)
	}
	got = probeExits([]string{"p0", "p1", "p2"}, 1)
	if fmt.Sprint(got) != fmt.Sprint([]string{"p1", "p2", "p0"}) {
		t.Fatalf("account 1 order = %v, want p1,p2,p0", got)
	}
	got = probeExits(nil, 0)
	if len(got) != 1 || got[0] != "" {
		t.Fatalf("empty pool order = %v, want a single direct attempt", got)
	}
}

func newFakeProxy(t *testing.T, hits *atomic.Int64) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)

		outbound, errNew := http.NewRequest(r.Method, r.URL.String(), r.Body)
		if errNew != nil {
			http.Error(w, errNew.Error(), http.StatusBadGateway)
			return
		}
		outbound.Header = r.Header.Clone()
		response, errDo := http.DefaultTransport.RoundTrip(outbound)
		if errDo != nil {
			http.Error(w, errDo.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		for name, values := range response.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func harvestTestConfig(t *testing.T) (pluginConfig, probeCredential, *probeClientPool) {
	t.Helper()
	cfg := pluginConfig{
		StoreDir:       t.TempDir(),
		TemplateLength: 292,
		ReplaceLength:  312,
		TTLSeconds:     3600,
	}
	cred := probeCredential{name: probeTestAccount, accessToken: "token-a", accountID: "acct-a"}
	pool := newProbeClientPool()
	t.Cleanup(pool.closeIdle)

	previous := probeExitPause
	probeExitPause = time.Millisecond
	t.Cleanup(func() { probeExitPause = previous })
	return cfg, cred, pool
}

func TestProbeAdvancesToNextExitOn312(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLenSeq = []int{312, 292}
	upstream.setCookies = []string{"__cflb=cf; Path=/", "__oailb=lb; Path=/"}
	setUpstream(t, upstream.server.URL)

	var hitsA, hitsB atomic.Int64
	exitA := newFakeProxy(t, &hitsA)
	exitB := newFakeProxy(t, &hitsB)
	cfg, cred, pool := harvestTestConfig(t)

	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, []string{exitA, exitB}, nil, 0) {
		t.Fatal("probeHarvestBucket reported no upstream call at all")
	}

	if poolEntryCount() == 0 {
		t.Fatal("no pair pooled: the 312 on the first exit ended the attempt instead of moving to the second")
	}
	if got := upstream.count(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2 (one per exit)", got)
	}
	if hitsA.Load() != 1 || hitsB.Load() != 1 {
		t.Fatalf("exit hits A=%d B=%d, want 1 each -- the pool was not walked in order", hitsA.Load(), hitsB.Load())
	}
}

func TestProbeStopsAfterWalkingThePoolAndCoolsDown(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLen = 312
	setUpstream(t, upstream.server.URL)

	var hitsA, hitsB atomic.Int64
	exitA := newFakeProxy(t, &hitsA)
	exitB := newFakeProxy(t, &hitsB)
	exits := []string{exitA, exitB}
	cfg, cred, pool := harvestTestConfig(t)

	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, exits, nil, 0) {
		t.Fatal("the first pass made no upstream call")
	}
	if got := upstream.count(); got != 2 {
		t.Fatalf("first pass made %d upstream calls, want 2 (one per exit)", got)
	}

	if probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, exits, nil, 0) {
		t.Fatal("a second pass inside the cooldown still fired; this is the runaway-retry defect")
	}
	if got := upstream.count(); got != 2 {
		t.Fatalf("upstream calls grew to %d inside the cooldown, want it pinned at 2", got)
	}
}

func TestProbeNewExitIsEligibleImmediately(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLenSeq = []int{312, 312, 292}
	upstream.setCookies = []string{"__cflb=cf; Path=/", "__oailb=lb; Path=/"}
	setUpstream(t, upstream.server.URL)

	var hitsA, hitsB, hitsC atomic.Int64
	exitA := newFakeProxy(t, &hitsA)
	exitB := newFakeProxy(t, &hitsB)
	cfg, cred, pool := harvestTestConfig(t)

	probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, []string{exitA, exitB}, nil, 0)
	if upstream.count() != 2 {
		t.Fatalf("setup: expected the pool to be walked once, got %d calls", upstream.count())
	}

	exitC := newFakeProxy(t, &hitsC)
	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, []string{exitA, exitB, exitC}, nil, 0) {
		t.Fatal("adding an exit did not make the bucket fireable again")
	}

	if poolEntryCount() == 0 {
		t.Fatal("the new exit did not mint a pair")
	}
	if got := upstream.count(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3: only the new exit should have fired", got)
	}
	if hitsA.Load() != 1 || hitsB.Load() != 1 {
		t.Fatalf("a cooling exit was re-dialed: A=%d B=%d, want 1 each", hitsA.Load(), hitsB.Load())
	}
	if hitsC.Load() != 1 {
		t.Fatalf("the new exit was dialed %d times, want 1", hitsC.Load())
	}
}

func TestProbe429StopsTheWalkAndRestsTheAccount(t *testing.T) {
	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.status = http.StatusTooManyRequests
	setUpstream(t, upstream.server.URL)

	var hitsA, hitsB, hitsC atomic.Int64
	exits := []string{
		newFakeProxy(t, &hitsA),
		newFakeProxy(t, &hitsB),
		newFakeProxy(t, &hitsC),
	}
	cfg, cred, pool := harvestTestConfig(t)

	probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, exits, nil, 0)

	if got := upstream.count(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1: a 429 must end the walk, not advance to the next exit", got)
	}
	if probeAccountReady(cred.name, time.Now()) {
		t.Fatal("the account was not rested after a 429, so the next bucket will hit it again immediately")
	}

	probeHarvestBucket(context.Background(), cfg, pool, cred, "another-model", exits, nil, 0)
	if got := upstream.count(); got != 1 {
		t.Fatalf("upstream calls grew to %d while the account was resting", got)
	}
}

func TestProbeSerialisesOneAccountsBuckets(t *testing.T) {
	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.hold = 40 * time.Millisecond
	setUpstream(t, upstream.server.URL)

	cfg, cred, pool := harvestTestConfig(t)
	creds := map[string]probeCredential{cred.name: cred}
	targets := []probeTarget{
		{account: cred.name, model: "m-1"},
		{account: cred.name, model: "m-2"},
		{account: cred.name, model: "m-3"},
	}

	probeFireBatch(context.Background(), cfg, pool, creds, targets,
		map[string]int{cred.name: 0}, nil, nil, false)

	if got := upstream.count(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3 (one per bucket)", got)
	}
	if peak := upstream.peakInFlight(); peak != 1 {
		t.Fatalf("peak concurrent requests against ONE credential = %d, want 1", peak)
	}
}

func TestProbeFallsThroughToNextExitOnTransportFailure(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.setCookies = []string{"__cflb=cf; Path=/", "__oailb=lb; Path=/"}
	setUpstream(t, upstream.server.URL)

	cfg := pluginConfig{
		StoreDir:       t.TempDir(),
		TemplateLength: 292,
		ReplaceLength:  312,
		TTLSeconds:     3600,
	}
	cred := probeCredential{name: probeTestAccount, accessToken: "token-a", accountID: "acct-a"}
	pool := newProbeClientPool()
	defer pool.closeIdle()

	probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, []string{"http://127.0.0.1:1", ""}, nil, 0)

	if poolEntryCount() == 0 {
		t.Fatal("no pair pooled, so the fallback to the second exit did not happen")
	}
	if upstream.count() != 1 {
		t.Fatalf("upstream received %d calls, want 1 (only the working exit)", upstream.count())
	}
	if !strings.Contains(strings.Join(probeRunSnapshot().Lines, "\n"), "trying next") {
		t.Fatal("no transport-failure fallthrough was logged")
	}
}

func TestProbeRenewsBucketNearingExpiry(t *testing.T) {

	resetProbeRunner(t)
	fastRenew(t, 15*time.Millisecond, 2*time.Hour, time.Millisecond)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a"})
	upstream := newFakeUpstream(t)
	setUpstream(t, upstream.server.URL)

	mustConfigure(t, probeTestConfig(probeTestOptions(t.TempDir(), fake.server.URL)))
	startProbeRun(t)
	waitForInitialFill(t)
	waitUntil(t, "a renewal fire", func() bool { return upstream.count() >= 2 })
}

func TestProbeRunNeverLeaksAProxyPassword(t *testing.T) {

	resetProbeRunner(t)
	fake := newFakeCPA(t, fakeCredSeed{name: probeTestAccount, accountID: "acct-a"})
	upstream := newFakeUpstream(t)
	setUpstream(t, upstream.server.URL)

	opts := probeTestOptions(t.TempDir(), fake.server.URL)

	opts.proxies = []string{"http://prober:" + testProxySecret + "@127.0.0.1:1"}
	mustConfigure(t, probeTestConfig(opts))
	startProbeRun(t)
	waitForInitialFill(t)

	run := probeRunSnapshot()
	for _, text := range append(append([]string(nil), run.Lines...), run.Error, run.Current) {
		if strings.Contains(text, testProxySecret) {
			t.Fatalf("a proxy password reached the run state: %q", text)
		}
	}
	if !strings.Contains(strings.Join(run.Lines, "\n"), "***@") {
		t.Fatal("no masked proxy appears anywhere, so this test proved nothing")
	}
}

func shrinkRotating(t *testing.T, attempts int, cooldown time.Duration) {
	t.Helper()
	prevAttempts, prevCooldown := probeRotatingAttempts, probeRotatingCooldown
	probeRotatingAttempts, probeRotatingCooldown = attempts, cooldown
	t.Cleanup(func() {
		probeRotatingAttempts, probeRotatingCooldown = prevAttempts, prevCooldown
	})
}

func TestProbeRotatingRetriesOneEntryForAFreshAddress(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLenSeq = []int{312, 312, 292}
	upstream.setCookies = []string{"__cflb=cf; Path=/", "__oailb=lb; Path=/"}
	setUpstream(t, upstream.server.URL)
	cfg, cred, pool := harvestTestConfig(t)
	shrinkRotating(t, 5, time.Minute)

	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{""}, 0) {
		t.Fatal("probeHarvestBucket reported no upstream call at all")
	}
	if poolEntryCount() == 0 {
		t.Fatal("no pair pooled: the rotating pool stopped at the first 312 instead of asking for another address")
	}
	if got := upstream.count(); got != 3 {
		t.Fatalf("upstream calls = %d, want 3 (two throttled addresses, then one that was not)", got)
	}
}

func TestProbeRotatingStopsAtItsBudgetAndRestsBriefly(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLen = 312
	setUpstream(t, upstream.server.URL)
	cfg, cred, pool := harvestTestConfig(t)
	shrinkRotating(t, 4, time.Hour)

	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{""}, 0) {
		t.Fatal("the first pass made no upstream call")
	}
	if got := upstream.count(); got != 4 {
		t.Fatalf("first pass made %d calls, want 4 (the whole budget)", got)
	}
	if probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{""}, 0) {
		t.Fatal("a second pass inside the rest window still fired")
	}
	if got := upstream.count(); got != 4 {
		t.Fatalf("upstream calls = %d after the second pass, want 4 -- the rest window is not holding", got)
	}

	shrinkRotating(t, 4, time.Nanosecond)
	probeCooldownSet(probeRotatingExit, cred.name, probeTestModel, time.Now().Add(-time.Second))
	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{""}, 0) {
		t.Fatal("the bucket never became eligible again after its rotating rest expired")
	}
}

func TestProbeRotatingStopsOnAccountLimit(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.status = http.StatusTooManyRequests
	setUpstream(t, upstream.server.URL)
	cfg, cred, pool := harvestTestConfig(t)
	shrinkRotating(t, 6, time.Minute)

	probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{""}, 0)

	if got := upstream.count(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 -- a 429 must end the rotating attempt, not burn the budget", got)
	}
	if probeAccountReady(cred.name, time.Now()) {
		t.Fatal("the account was not rested after a 429")
	}
}

func TestProbeRotatingPoolSuppressesTheImplicitDirectExit(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLen = 292
	setUpstream(t, upstream.server.URL)
	cfg, cred, pool := harvestTestConfig(t)
	shrinkRotating(t, 1, time.Minute)

	var hits atomic.Int64
	rotatingExit := newFakeProxy(t, &hits)

	probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel, nil, []string{rotatingExit}, 0)

	if got := upstream.count(); got != 1 {
		t.Fatalf("upstream calls = %d, want exactly 1 -- an extra call means the direct exit fired too", got)
	}
	if hits.Load() != 1 {
		t.Fatalf("the rotating exit carried %d request(s), want 1; the harvest went out direct instead", hits.Load())
	}
}

func TestProbeStaticPoolIsTriedBeforeRotating(t *testing.T) {

	resetProbeRunner(t)
	upstream := newFakeUpstream(t)
	upstream.tsLenSeq = []int{312, 292}
	upstream.setCookies = []string{"__cflb=cf; Path=/", "__oailb=lb; Path=/"}
	setUpstream(t, upstream.server.URL)
	cfg, cred, pool := harvestTestConfig(t)
	shrinkRotating(t, 3, time.Minute)

	var staticHits, rotatingHits atomic.Int64
	staticExit := newFakeProxy(t, &staticHits)
	rotatingExit := newFakeProxy(t, &rotatingHits)

	if !probeHarvestBucket(context.Background(), cfg, pool, cred, probeTestModel,
		[]string{staticExit}, []string{rotatingExit}, 0) {
		t.Fatal("no upstream call was made")
	}
	if poolEntryCount() == 0 {
		t.Fatal("no pair pooled although the rotating pool had a good address")
	}
	if staticHits.Load() != 1 || rotatingHits.Load() != 1 {
		t.Fatalf("static=%d rotating=%d, want 1 each -- the static exit must be spent first",
			staticHits.Load(), rotatingHits.Load())
	}
}

func TestProbeClientReadsLargeManagementDocument(t *testing.T) {
	big := strings.Repeat("x", 200<<10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"files":[%q]}`, big)
	}))
	defer server.Close()

	client := &probeClient{baseURL: server.URL, mgmtKey: "k", http: server.Client()}
	res, err := client.call(context.Background(), http.MethodGet, probeRouteAuthFiles, "k", nil, probeMgmtTimeout)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	var doc struct {
		Files []string `json:"files"`
	}
	if errUnmarshal := json.Unmarshal(res.body, &doc); errUnmarshal != nil {
		t.Fatalf("the large document was not returned whole: %v", errUnmarshal)
	}
	if len(doc.Files) != 1 || len(doc.Files[0]) != len(big) {
		t.Fatalf("document content was altered: files=%d", len(doc.Files))
	}
}

func TestProbeClientReportsOversizeRatherThanMisParsing(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, probeMgmtMaxBodyBytes+2))
	}))
	defer server.Close()

	client := &probeClient{baseURL: server.URL, mgmtKey: "k", http: server.Client()}
	_, err := client.call(context.Background(), http.MethodGet, probeRouteAuthFiles, "k", nil, probeMgmtTimeout)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("an oversize response was not reported as such: %v", err)
	}
}
