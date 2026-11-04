package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// vlog formats a vinyllog record line the way vinyllog pads its columns.
func vlog(tag, value string) string {
	return fmt.Sprintf("-   %-14s %s", tag, value)
}

// vlogEntry wraps records in the header, End and trailing empty line
// vinyllog prints around each request.
func vlogEntry(records ...string) string {
	lines := []string{"*   << Request  >> 2392453"}
	lines = append(lines, vlog("Begin", "req 2392452 rxreq"))
	lines = append(lines, records...)
	lines = append(lines, vlog("End", ""), "")
	return strings.Join(lines, "\n") + "\n"
}

// parse runs runParser on input and returns the published messages.
func parse(t *testing.T, input string) []purgeMessage {
	t.Helper()

	msgChan := make(chan []byte, 10)
	logger := zerolog.New(zerolog.NewTestWriter(t))
	scanner := bufio.NewScanner(strings.NewReader(input))

	if err := runParser(scanner, logger, false, msgChan, "node1", "cache-vinyl-1"); err != nil {
		t.Fatalf("runParser returned error: %v", err)
	}
	close(msgChan)

	var msgs []purgeMessage
	for b := range msgChan {
		var pm purgeMessage
		if err := json.Unmarshal(b, &pm); err != nil {
			t.Fatalf("unable to unmarshal published message %q: %v", b, err)
		}
		msgs = append(msgs, pm)
	}
	return msgs
}

func TestRunParserPublishesMethod(t *testing.T) {
	tests := []struct {
		method string
	}{
		{"PURGE"},
		{"BAN"},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			msgs := parse(t, vlogEntry(
				vlog("ReqStart", "192.0.2.10 62353 a0"),
				vlog("ReqMethod", tc.method),
				vlog("ReqURL", "/"),
				vlog("ReqHeader", "host: example.com"),
				vlog("ReqHeader", "Ban-Url-Regex: ^/wiki/spaces/DOCS/"),
				vlog("ReqHeader", "X-Forwarded-For: 192.0.2.10"),
				vlog("VCL_call", "RECV"),
				vlog("ReqHeader", "X-Forwarded-Proto: https"),
				vlog("VCL_call", "SYNTH"),
			))

			if len(msgs) != 1 {
				t.Fatalf("got %d messages, want 1", len(msgs))
			}
			pm := msgs[0]
			if pm.Method != tc.method {
				t.Errorf("Method = %q, want %q", pm.Method, tc.method)
			}
			if pm.URL == nil || pm.URL.String() != "http://example.com/" {
				t.Errorf("URL = %v, want http://example.com/", pm.URL)
			}
			if got := pm.Header.Get("Ban-Url-Regex"); got != "^/wiki/spaces/DOCS/" {
				t.Errorf("Ban-Url-Regex = %q, want %q", got, "^/wiki/spaces/DOCS/")
			}
			if !pm.ClientTLS {
				t.Errorf("ClientTLS = false, want true")
			}
		})
	}
}

// The method is taken as received; a change made by the tenant VCL is
// ignored like changes to the URL and headers.
func TestRunParserUsesMethodAsReceived(t *testing.T) {
	msgs := parse(t, vlogEntry(
		vlog("ReqStart", "192.0.2.10 62353 a0"),
		vlog("ReqMethod", "BAN"),
		vlog("ReqURL", "/"),
		vlog("ReqHeader", "host: example.com"),
		vlog("VCL_call", "RECV"),
		vlog("ReqMethod", "PURGE"),
		vlog("VCL_call", "SYNTH"),
	))

	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].Method != "BAN" {
		t.Errorf("Method = %q, want BAN", msgs[0].Method)
	}
}

// A request that only became a PURGE or BAN inside the VCL can't be
// replayed with its original method, so it is not published.
func TestRunParserSkipsMethodChangedByVCL(t *testing.T) {
	msgs := parse(t, vlogEntry(
		vlog("ReqStart", "192.0.2.10 62353 a0"),
		vlog("ReqMethod", "GET"),
		vlog("ReqURL", "/"),
		vlog("ReqHeader", "host: example.com"),
		vlog("VCL_call", "RECV"),
		vlog("ReqMethod", "BAN"),
		vlog("VCL_call", "SYNTH"),
	))

	if len(msgs) != 0 {
		t.Fatalf("got %d messages, want 0: %+v", len(msgs), msgs)
	}
}

func TestRunParserSkipsRequestsFromPurger(t *testing.T) {
	msgs := parse(t, vlogEntry(
		vlog("ReqStart", "192.0.2.10 40000 purger"),
		vlog("ReqMethod", "BAN"),
		vlog("ReqURL", "/"),
		vlog("ReqHeader", "host: example.com"),
		vlog("VCL_call", "RECV"),
		vlog("VCL_call", "SYNTH"),
	))

	if len(msgs) != 0 {
		t.Fatalf("got %d messages, want 0: %+v", len(msgs), msgs)
	}
}

func TestNewLocalRequestUsesMessageMethod(t *testing.T) {
	for _, method := range []string{"PURGE", "BAN"} {
		t.Run(method, func(t *testing.T) {
			pm := purgeMessage{
				Method: method,
				URL:    &url.URL{Scheme: "http", Host: "example.com", Path: "/wiki/"},
				Header: http.Header{
					"Host":          {"example.com"},
					"Ban-Url-Regex": {"^/wiki/spaces/DOCS/"},
				},
				Sender: "node2",
			}

			req, err := newLocalRequest(context.Background(), pm)
			if err != nil {
				t.Fatalf("newLocalRequest returned error: %v", err)
			}
			if req.Method != method {
				t.Errorf("Method = %q, want %q", req.Method, method)
			}
			if req.URL.RequestURI() != "/wiki/" {
				t.Errorf("RequestURI = %q, want /wiki/", req.URL.RequestURI())
			}
			if req.Host != "example.com" {
				t.Errorf("Host = %q, want example.com", req.Host)
			}
			if got := req.Header.Get("Ban-Url-Regex"); got != "^/wiki/spaces/DOCS/" {
				t.Errorf("Ban-Url-Regex = %q, want %q", got, "^/wiki/spaces/DOCS/")
			}
			if got := req.Header.Get(purgerOriginHeader); got != "node2" {
				t.Errorf("%s = %q, want node2", purgerOriginHeader, got)
			}
		})
	}
}

func TestNewLocalRequestRejectsOtherMethods(t *testing.T) {
	for _, method := range []string{"", "GET", "POST", "purge"} {
		t.Run(method, func(t *testing.T) {
			pm := purgeMessage{
				Method: method,
				URL:    &url.URL{Scheme: "http", Host: "example.com", Path: "/"},
				Header: http.Header{"Host": {"example.com"}},
			}

			req, err := newLocalRequest(context.Background(), pm)
			if err == nil {
				t.Fatalf("newLocalRequest accepted method %q, built %s request", method, req.Method)
			}
		})
	}
}
