package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	testWebhookUser = "webhookuser"
	testWebhookPass = "webhookpass"
	testCaller      = "+46701234567"
)

type mockStore struct {
	called  bool
	input   *s3.PutObjectInput
	err     error
	objects map[string][]byte
	gets    int
}

func (m *mockStore) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.called = true
	m.input = input
	return &s3.PutObjectOutput{}, m.err
}

func (m *mockStore) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.gets++
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
	}, nil
}

type mockTranscriber struct {
	called atomic.Bool
	text   string
	err    error
}

func (m *mockTranscriber) Transcribe(_ context.Context, _ []byte, _ string) (string, error) {
	m.called.Store(true)
	return m.text, m.err
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parsing %q: %v", raw, err)
	}
	return u
}

func testConfig(t *testing.T) Configuration {
	return Configuration{
		Host:           mustParseURL(t, "https://example.com"),
		VoicemailAudio: "https://example.com/greeting.mp3",
		ElksUserName:   "testuser",
		ElksPassword:   "testpass",
		S3Endpoint:     "https://s3.example.com",
		S3BucketName:   "test-bucket",
		SMSFrom:        "Terry",
		SMSTo:          "+46709876543",
		WebhookUser:    testWebhookUser,
		WebhookPass:    testWebhookPass,
	}
}

// fakeElks is a fake 46elks API that serves a recording and records sent SMS.
type fakeElks struct {
	*httptest.Server
	wav       []byte
	wavStatus int
	smsStatus int
	// wavHook, if set, runs before the recording is served.
	wavHook func()

	mu  sync.Mutex
	sms []url.Values
}

func newFakeElks(t *testing.T, wav []byte) *fakeElks {
	t.Helper()
	f := &fakeElks{wav: wav, wavStatus: http.StatusOK, smsStatus: http.StatusOK}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeElks) serveHTTP(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "testuser" || pass != "testpass" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == "GET" && strings.HasSuffix(r.URL.Path, ".wav"):
		if f.wavHook != nil {
			f.wavHook()
		}
		w.WriteHeader(f.wavStatus)
		_, _ = w.Write(f.wav)
	case r.Method == "POST" && r.URL.Path == "/a1/sms":
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.sms = append(f.sms, r.PostForm)
		f.mu.Unlock()
		w.WriteHeader(f.smsStatus)
		_, _ = w.Write([]byte(`{"status":"created"}`))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeElks) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var msgs []string
	for _, v := range f.sms {
		msgs = append(msgs, v.Get("message"))
	}
	return msgs
}

func (f *fakeElks) onlyMessage(t *testing.T) string {
	t.Helper()
	msgs := f.messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 SMS, got %d: %q", len(msgs), msgs)
	}
	return msgs[0]
}

// voicemailFixture wires a server to a fake 46elks.
type voicemailFixture struct {
	srv   *server
	store *mockStore
	elks  *fakeElks
}

func newVoicemailFixture(t *testing.T, wav []byte, tr transcriber) *voicemailFixture {
	t.Helper()
	elks := newFakeElks(t, wav)
	store := &mockStore{}
	srv := newServer(testConfig(t), elks.Client(), store, tr)
	srv.elksAPI = mustParseURL(t, elks.URL)
	return &voicemailFixture{srv: srv, store: store, elks: elks}
}

// recordingURL returns the link to the recording that was uploaded.
func (f *voicemailFixture) recordingURL(t *testing.T) string {
	t.Helper()
	if !f.store.called {
		t.Fatal("expected recording to be uploaded")
	}
	return "https://example.com/recordings/" + strings.TrimPrefix(*f.store.input.Key, "voicemail/")
}

// post sends a voicemail callback and waits for background work to finish.
func (f *voicemailFixture) post(t *testing.T, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	w := postVoicemail(t, f.srv, form)
	if err := f.srv.waitForBackground(t.Context()); err != nil {
		t.Fatalf("waiting for background work: %v", err)
	}
	return w
}

func (f *voicemailFixture) defaultForm() url.Values {
	return url.Values{
		"from": {testCaller},
		"wav":  {f.elks.URL + "/a1/recordings/r1.wav"},
	}
}

func postVoicemail(t *testing.T, h http.Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/voicemail", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(testWebhookUser, testWebhookPass)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestHealth(t *testing.T) {
	srv := newServer(testConfig(t), http.DefaultClient, &mockStore{}, nil)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestBasicAuth(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		user, pass string
		setAuth    bool
		want       int
	}{
		{"no credentials", "/incoming_call", "", "", false, http.StatusUnauthorized},
		{"wrong credentials", "/incoming_call", "wrong", "wrong", true, http.StatusUnauthorized},
		{"wrong password", "/incoming_call", testWebhookUser, "wrong", true, http.StatusUnauthorized},
		{"valid credentials", "/incoming_call", testWebhookUser, testWebhookPass, true, http.StatusOK},
		{"voicemail without credentials", "/voicemail", "", "", false, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newServer(testConfig(t), http.DefaultClient, &mockStore{}, nil)
			req := httptest.NewRequestWithContext(t.Context(), "POST", tt.path, nil)
			if tt.setAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)

			if w.Code != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, w.Code)
			}
		})
	}
}

func incomingCall(t *testing.T, cfg Configuration) IncomingResponse {
	t.Helper()
	srv := newServer(cfg, http.DefaultClient, &mockStore{}, nil)
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/incoming_call", nil)
	req.SetBasicAuth(cfg.WebhookUser, cfg.WebhookPass)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("expected JSON content type, got %q", ct)
	}
	var resp IncomingResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return resp
}

func TestIncomingCall(t *testing.T) {
	cfg := testConfig(t)
	resp := incomingCall(t, cfg)

	if resp.Play != cfg.VoicemailAudio {
		t.Errorf("expected play=%q, got %q", cfg.VoicemailAudio, resp.Play)
	}
	if resp.Next.SilenceDetection != "no" {
		t.Errorf("expected silencedetection=no, got %q", resp.Next.SilenceDetection)
	}
	expected := "https://webhookuser:webhookpass@example.com/voicemail"
	if resp.Next.Record != expected {
		t.Errorf("expected callback URL %q, got %q", expected, resp.Next.Record)
	}
}

func TestIncomingCallEscapesCredentialsInCallbackURL(t *testing.T) {
	cfg := testConfig(t)
	cfg.WebhookPass = "p@ss:w/rd?"
	resp := incomingCall(t, cfg)

	expected := "https://webhookuser:p%40ss%3Aw%2Frd%3F@example.com/voicemail"
	if resp.Next.Record != expected {
		t.Fatalf("expected callback URL %q, got %q", expected, resp.Next.Record)
	}
	u, err := url.Parse(resp.Next.Record)
	if err != nil {
		t.Fatalf("callback URL does not parse: %v", err)
	}
	if pass, _ := u.User.Password(); pass != cfg.WebhookPass {
		t.Fatalf("expected password to round-trip, got %q", pass)
	}
}

func TestIncomingCallRejectsGET(t *testing.T) {
	srv := newServer(testConfig(t), http.DefaultClient, &mockStore{}, nil)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/incoming_call", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code == http.StatusOK {
		t.Fatal("expected non-200 for GET request")
	}
}

func TestVoicemailMissingFields(t *testing.T) {
	srv := newServer(testConfig(t), http.DefaultClient, &mockStore{}, nil)

	tests := []struct {
		name   string
		values url.Values
	}{
		{"missing both", url.Values{}},
		{"missing wav", url.Values{"from": {"+46123456"}}},
		{"missing from", url.Values{"wav": {"https://api.46elks.com/a1/recordings/r1.wav"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if w := postVoicemail(t, srv, tt.values); w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", w.Code)
			}
		})
	}
}

func TestVoicemailRejectsUntrustedWAVURL(t *testing.T) {
	var contacted atomic.Bool
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contacted.Store(true)
	}))
	defer attacker.Close()

	store := &mockStore{}
	srv := newServer(testConfig(t), attacker.Client(), store, nil)

	tests := []string{
		attacker.URL + "/recording.wav",
		"http://api.46elks.com/a1/recordings/r1.wav",
		"https://api.46elks.com.evil.example/r1.wav",
		"https://api.46elks.com@evil.example/r1.wav",
		"https://user:pass@api.46elks.com/r1.wav",
		"://not a url",
	}
	for _, wav := range tests {
		t.Run(wav, func(t *testing.T) {
			w := postVoicemail(t, srv, url.Values{"from": {testCaller}, "wav": {wav}})
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d", w.Code)
			}
		})
	}
	if contacted.Load() {
		t.Error("untrusted WAV host must not be contacted")
	}
	if store.called {
		t.Error("nothing should be uploaded for untrusted WAV URLs")
	}
}

func TestVoicemailAcceptsElksWAVURL(t *testing.T) {
	srv := newServer(testConfig(t), http.DefaultClient, &mockStore{}, nil)
	if err := srv.validateWAVURL("https://api.46elks.com/a1/recordings/r1.wav"); err != nil {
		t.Fatalf("expected 46elks URL to be accepted, got %v", err)
	}
}

func assertPrivateUpload(t *testing.T, in *s3.PutObjectInput, wantBody []byte) {
	t.Helper()
	if *in.Bucket != "test-bucket" {
		t.Errorf("expected bucket=test-bucket, got %q", *in.Bucket)
	}
	if *in.ContentType != "audio/wav" {
		t.Errorf("expected content-type=audio/wav, got %q", *in.ContentType)
	}
	if in.ACL != "" {
		t.Errorf("expected recordings to be private, got ACL %q", in.ACL)
	}
	if !regexp.MustCompile(`^voicemail/\d{14}-[A-Z2-7]{26}\.wav$`).MatchString(*in.Key) {
		t.Errorf("unexpected object key %q", *in.Key)
	}
	uploadedBody, _ := io.ReadAll(in.Body)
	if string(uploadedBody) != string(wantBody) {
		t.Errorf("uploaded data mismatch: got %q", uploadedBody)
	}
}

func TestVoicemailSendsTranscriptionBySMS(t *testing.T) {
	wavData := []byte("fake-wav-data")
	tr := &mockTranscriber{text: " Hi, it's Anna. Call me back. "}
	f := newVoicemailFixture(t, wavData, tr)

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	assertPrivateUpload(t, f.store.input, wavData)
	if !tr.called.Load() {
		t.Fatal("expected transcriber to be called")
	}

	f.elks.mu.Lock()
	sms := f.elks.sms
	f.elks.mu.Unlock()
	if len(sms) != 1 {
		t.Fatalf("expected 1 SMS, got %d", len(sms))
	}
	if got := sms[0].Get("from"); got != "Terry" {
		t.Errorf("expected from=Terry, got %q", got)
	}
	if got := sms[0].Get("to"); got != "+46709876543" {
		t.Errorf("expected to=+46709876543, got %q", got)
	}
	want := "Voicemail from +46701234567:\nHi, it's Anna. Call me back.\n" + f.recordingURL(t)
	if got := sms[0].Get("message"); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestVoicemailSplitsLongTranscriptionIntoSeveralSMS(t *testing.T) {
	tr := &mockTranscriber{text: strings.Repeat("blah ", maxGSMLength/2)}
	f := newVoicemailFixture(t, []byte("fake-wav-data"), tr)

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	msgs := f.elks.messages()
	if len(msgs) != 3 {
		t.Fatalf("expected 3 SMS, got %d", len(msgs))
	}
	for i, prefix := range []string{"(1/3) Voicemail from", "(2/3) blah", "(3/3) blah"} {
		if !strings.HasPrefix(msgs[i], prefix) {
			t.Errorf("SMS %d: expected prefix %q, got %q", i+1, prefix, msgs[i][:20])
		}
	}
}

func TestVoicemailRespondsBeforeTranscribing(t *testing.T) {
	release := make(chan struct{})
	tr := &blockingTranscriber{release: release}
	f := newVoicemailFixture(t, []byte("fake-wav"), tr)

	w := postVoicemail(t, f.srv, f.defaultForm())
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 while transcription is pending, got %d", w.Code)
	}
	if len(f.elks.messages()) != 0 {
		t.Error("voicemail handler waited for the SMS before responding")
	}

	close(release)
	if err := f.srv.waitForBackground(t.Context()); err != nil {
		t.Fatalf("waiting for background work: %v", err)
	}
	f.elks.onlyMessage(t)
}

type blockingTranscriber struct {
	release chan struct{}
}

func (b *blockingTranscriber) Transcribe(_ context.Context, _ []byte, _ string) (string, error) {
	// Time out rather than deadlock if the handler waits for transcription.
	select {
	case <-b.release:
	case <-time.After(2 * time.Second):
	}
	return "hello", nil
}

// stuckTranscriber never finishes until its context is cancelled.
type stuckTranscriber struct{}

func (stuckTranscriber) Transcribe(ctx context.Context, _ []byte, _ string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(5 * time.Second):
		return "too late", nil
	}
}

func TestDrainCancelsTranscriptionAndSendsFallback(t *testing.T) {
	f := newVoicemailFixture(t, []byte("fake-wav"), stuckTranscriber{})

	if w := postVoicemail(t, f.srv, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if err := f.srv.drain(10*time.Millisecond, 2*time.Second); err != nil {
		t.Fatalf("drain: %v", err)
	}
	want := "Voicemail from +46701234567 (transcription failed): " + f.recordingURL(t)
	if got := f.elks.onlyMessage(t); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestDrainWaitsForInFlightVoicemailCallbacks(t *testing.T) {
	f := newVoicemailFixture(t, []byte("fake-wav"), &mockTranscriber{text: "hello"})
	downloading := make(chan struct{})
	release := make(chan struct{})
	f.elks.wavHook = func() {
		close(downloading)
		<-release
	}

	go postVoicemail(t, f.srv, f.defaultForm())
	<-downloading
	time.AfterFunc(50*time.Millisecond, func() { close(release) })

	if err := f.srv.drain(2*time.Second, time.Second); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := f.elks.onlyMessage(t); !strings.Contains(got, "hello") {
		t.Errorf("expected transcription in SMS, got %q", got)
	}
}

func TestDrainWaitsForFinishedWork(t *testing.T) {
	f := newVoicemailFixture(t, []byte("fake-wav"), &mockTranscriber{text: "hello"})

	if w := postVoicemail(t, f.srv, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if err := f.srv.drain(2*time.Second, time.Second); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := f.elks.onlyMessage(t); !strings.Contains(got, "hello") {
		t.Errorf("expected transcription in SMS, got %q", got)
	}
}

func TestVoicemailWithoutSpeech(t *testing.T) {
	f := newVoicemailFixture(t, []byte("fake-wav"), &mockTranscriber{text: "  "})

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	want := "Voicemail from +46701234567 (no speech detected): " + f.recordingURL(t)
	if got := f.elks.onlyMessage(t); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestVoicemailTranscriptionFailureStillNotifies(t *testing.T) {
	f := newVoicemailFixture(t, []byte("fake-wav"), &mockTranscriber{err: errors.New("API error")})

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	want := "Voicemail from +46701234567 (transcription failed): " + f.recordingURL(t)
	if got := f.elks.onlyMessage(t); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestVoicemailSkipsTranscriptionForLargeRecordings(t *testing.T) {
	tr := &mockTranscriber{text: "should not be used"}
	f := newVoicemailFixture(t, make([]byte, maxTranscriptionSize+1), tr)

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if tr.called.Load() {
		t.Error("transcriber should not be called for recordings over the upload limit")
	}
	if got := f.elks.onlyMessage(t); !strings.Contains(got, "transcription failed") {
		t.Errorf("expected fallback message, got %q", got)
	}
}

func TestVoicemailTooLarge(t *testing.T) {
	tr := &mockTranscriber{}
	f := newVoicemailFixture(t, make([]byte, maxWAVSize+1), tr)

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized WAV, got %d", w.Code)
	}
	if tr.called.Load() {
		t.Fatal("oversized recordings must not be transcribed")
	}
	if f.store.called {
		t.Fatal("oversized recordings must not be uploaded")
	}
}

func TestVoicemailElksErrorStatus(t *testing.T) {
	tr := &mockTranscriber{}
	f := newVoicemailFixture(t, nil, tr)
	f.elks.wavStatus = http.StatusNotFound

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when 46elks returns an error, got %d", w.Code)
	}
	if tr.called.Load() {
		t.Fatal("error responses from 46elks must not be transcribed")
	}
	if f.store.called {
		t.Fatal("error responses from 46elks must not be uploaded")
	}
}

func TestVoicemailS3Failure(t *testing.T) {
	tr := &mockTranscriber{}
	f := newVoicemailFixture(t, []byte("fake-wav"), tr)
	f.store.err = io.ErrUnexpectedEOF

	if w := f.post(t, f.defaultForm()); w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on S3 failure, got %d", w.Code)
	}
	if tr.called.Load() || len(f.elks.messages()) != 0 {
		t.Fatal("nothing should be transcribed or sent when the recording can't be stored")
	}
}

func TestRecordingServesObject(t *testing.T) {
	name := "20260101120000-ABCDEFGHIJKLMNOPQRSTUVWXYZ.wav"
	store := &mockStore{objects: map[string][]byte{"voicemail/" + name: []byte("wav-bytes")}}
	srv := newServer(testConfig(t), http.DefaultClient, store, nil)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/recordings/"+name, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "audio/wav" {
		t.Errorf("expected audio/wav, got %q", ct)
	}
	if cl := w.Header().Get("Content-Length"); cl != "9" {
		t.Errorf("expected Content-Length 9, got %q", cl)
	}
	if w.Body.String() != "wav-bytes" {
		t.Errorf("unexpected body %q", w.Body.String())
	}
}

func TestRecordingNotFound(t *testing.T) {
	store := &mockStore{}
	srv := newServer(testConfig(t), http.DefaultClient, store, nil)

	tests := []struct {
		name      string
		path      string
		wantFetch bool
	}{
		{"missing object", "/recordings/20260101120000-ABCDEFGHIJKLMNOPQRSTUVWXYZ.wav", true},
		{"invalid name", "/recordings/secret.wav", false},
		{"short id", "/recordings/20260101120000-abcd1234.wav", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.gets = 0
			req := httptest.NewRequestWithContext(t.Context(), "GET", tt.path, nil)
			w := httptest.NewRecorder()
			srv.ServeHTTP(w, req)

			if w.Code != http.StatusNotFound {
				t.Fatalf("expected 404, got %d", w.Code)
			}
			if fetched := store.gets > 0; fetched != tt.wantFetch {
				t.Errorf("expected fetch=%v, got %v", tt.wantFetch, fetched)
			}
		})
	}
}

func TestSendSMSReturnsErrorOnNon2xx(t *testing.T) {
	f := newVoicemailFixture(t, nil, nil)
	f.elks.smsStatus = http.StatusForbidden

	if err := f.srv.sendSMS(t.Context(), "hello"); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected error mentioning 403, got %v", err)
	}
}

func TestSplitMessage(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		limit int
		want  []string
	}{
		{"fits", "hello world", 20, []string{"hello world"}},
		{"exact fit", "0123456789", 10, []string{"0123456789"}},
		{
			name:  "splits on word boundaries",
			text:  "aaaa bbbb cccc dddd eeee",
			limit: 16,
			want:  []string{"(1/3) aaaa bbbb", "(2/3) cccc dddd", "(3/3) eeee"},
		},
		{
			name:  "hard splits long words",
			text:  "abcdefghijklmnopqrst",
			limit: 12,
			want:  []string{"(1/4) abcdef", "(2/4) ghijkl", "(3/4) mnopqr", "(4/4) st"},
		},
		{
			name:  "counts characters, not bytes",
			text:  "åäö åäö åäö",
			limit: 11,
			want:  []string{"åäö åäö åäö"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitMessage(tt.text, tt.limit, func(rune) int { return 1 })
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestSplitMessageWeighsCharacters(t *testing.T) {
	cost := func(r rune) int {
		if r == '€' {
			return 2
		}
		return 1
	}
	// The 6 unit "(i/n) " prefix leaves room for three euro signs per part.
	got := splitMessage("€€€€ aaaa", 12, cost)
	want := []string{"(1/3) €€€", "(2/3) €", "(3/3) aaaa"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

// segments counts how many concatenated SMS segments s needs, packing whole
// characters into segments the way phones do.
func segments(s string) int {
	gsm, size := true, 153
	for _, r := range s {
		if !strings.ContainsRune(gsmBasic, r) && !strings.ContainsRune(gsmExtension, r) {
			gsm, size = false, 67
			break
		}
	}
	n, used := 1, 0
	for _, r := range s {
		c := utf16.RuneLen(r)
		if gsm {
			c = 1
			if strings.ContainsRune(gsmExtension, r) {
				c = 2
			}
		}
		if used+c > size {
			n, used = n+1, 0
		}
		used += c
	}
	return n
}

func TestSplitSMSKeepsPartsWithinTenSegments(t *testing.T) {
	tests := map[string]string{
		"gsm":             strings.Repeat("ord och några längre ordsammansättningar ", 200),
		"gsm extension":   strings.Repeat("€", 2000),
		"unicode":         strings.Repeat("He said \u201chi\u201d ", 300),
		"surrogate pairs": strings.Repeat("\U0001F600", 1000),
	}
	for name, text := range tests {
		t.Run(name, func(t *testing.T) {
			parts := splitSMS(text)
			if len(parts) < 2 {
				t.Fatalf("expected several parts, got %d", len(parts))
			}
			for i, p := range parts {
				if n := segments(p); n > 10 {
					t.Errorf("part %d needs %d segments", i+1, n)
				}
			}
		})
	}
}

func TestSplitSMSBoundaryPadding(t *testing.T) {
	// 335 emoji are 670 UTF-16 units but need eleven 67-unit segments.
	if got := splitSMS(strings.Repeat("\U0001F600", 335)); len(got) == 1 {
		t.Fatalf("expected the message to be split, got one part of %d segments", segments(got[0]))
	}
}

func TestSplitSMSShortMessage(t *testing.T) {
	if got := splitSMS("Voicemail from +46701234567: “hi”"); len(got) != 1 {
		t.Fatalf("expected a single part, got %q", got)
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"ELKS_USERNAME":   "elks",
		"ELKS_PASSWORD":   "elkspass",
		"OPENAI_API_KEY":  "sk-test",
		"S3_ACCESS_KEY":   "access",
		"S3_SECRET_KEY":   "secret",
		"S3_BUCKET_NAME":  "bucket",
		"S3_ENDPOINT":     "https://s3.example.com",
		"S3_REGION":       "fsn1",
		"SMS_FROM":        "Terry",
		"SMS_TO":          "+46709876543",
		"HOST":            "https://terry.example.com",
		"VOICEMAIL_AUDIO": "https://example.com/greeting.mp3",
		"WEBHOOK_USER":    "user",
		"WEBHOOK_PASS":    "pass",
	}
}

func TestLoadConfig(t *testing.T) {
	env := validEnv()
	cfg, err := loadConfig(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Host.String() != "https://terry.example.com" {
		t.Errorf("unexpected host %q", cfg.Host)
	}
	if cfg.OpenAIAPIKey != "sk-test" || cfg.SMSTo != "+46709876543" || cfg.VoicemailAudio != env["VOICEMAIL_AUDIO"] {
		t.Errorf("config not populated: %+v", cfg)
	}
	if cfg.Port != "3000" {
		t.Errorf("expected default port 3000, got %q", cfg.Port)
	}
	if cfg.TranscriptionModel != defaultTranscriptionModel {
		t.Errorf("expected default model %q, got %q", defaultTranscriptionModel, cfg.TranscriptionModel)
	}
}

func TestLoadConfigReportsAllMissingVariables(t *testing.T) {
	env := validEnv()
	delete(env, "HOST")
	delete(env, "OPENAI_API_KEY")
	delete(env, "SMS_TO")
	delete(env, "S3_SECRET_KEY")

	_, err := loadConfig(func(k string) string { return env[k] })
	if err == nil {
		t.Fatal("expected error")
	}
	for _, name := range []string{"HOST", "OPENAI_API_KEY", "SMS_TO", "S3_SECRET_KEY"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("expected error to mention %s, got %q", name, err)
		}
	}
}

func TestLoadConfigRejectsInvalidURLs(t *testing.T) {
	for _, name := range []string{"HOST", "S3_ENDPOINT", "VOICEMAIL_AUDIO"} {
		for _, value := range []string{"example.com", "hel1.your-objectstorage.com", "ftp://example.com", "https://"} {
			t.Run(name+"="+value, func(t *testing.T) {
				env := validEnv()
				env[name] = value
				_, err := loadConfig(func(k string) string { return env[k] })
				if err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("expected error mentioning %s, got %v", name, err)
				}
			})
		}
	}
}

// transcriptionRequest is what the fake OpenAI API received.
type transcriptionRequest struct {
	auth, model, filename string
	audio                 []byte
}

func newOpenAIServer(t *testing.T, got *transcriptionRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/audio/transcriptions" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		got.auth = r.Header.Get("Authorization")
		mr, err := r.MultipartReader()
		if err != nil {
			t.Errorf("reading multipart: %v", err)
			return
		}
		for part, err := mr.NextPart(); !errors.Is(err, io.EOF); part, err = mr.NextPart() {
			if err != nil {
				t.Errorf("reading multipart: %v", err)
				return
			}
			data, _ := io.ReadAll(part)
			switch part.FormName() {
			case "model":
				got.model = string(data)
			case "file":
				got.filename = part.FileName()
				got.audio = data
			}
		}
		_, _ = w.Write([]byte(`{"text":"hej hej"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOpenAITranscriber(t *testing.T) {
	var got transcriptionRequest
	api := newOpenAIServer(t, &got)

	ot := newOpenAITranscriber("sk-test", "gpt-4o-transcribe")
	ot.baseURL = api.URL
	text, err := ot.Transcribe(t.Context(), []byte("audio"), "voicemail.wav")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if text != "hej hej" {
		t.Errorf("expected transcription %q, got %q", "hej hej", text)
	}
	if got.auth != "Bearer sk-test" {
		t.Errorf("unexpected Authorization header %q", got.auth)
	}
	if got.model != "gpt-4o-transcribe" {
		t.Errorf("unexpected model %q", got.model)
	}
	if got.filename != "voicemail.wav" || string(got.audio) != "audio" {
		t.Errorf("unexpected file part %q: %q", got.filename, got.audio)
	}
}

func TestOpenAITranscriberErrorStatus(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad key"}`, http.StatusUnauthorized)
	}))
	defer api.Close()

	ot := newOpenAITranscriber("sk-test", "gpt-4o-transcribe")
	ot.baseURL = api.URL
	_, err := ot.Transcribe(t.Context(), []byte("audio"), "voicemail.wav")
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected error mentioning 401, got %v", err)
	}
}
