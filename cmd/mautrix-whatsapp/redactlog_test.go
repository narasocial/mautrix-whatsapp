// mautrix-whatsapp - A Matrix-WhatsApp puppeting bridge.
// Copyright (C) 2026 narasocial
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestRedactingWriter(t *testing.T) {
	var out bytes.Buffer
	w := newRedactingWriter(&out)
	log := zerolog.New(w).With().
		Str("login_id", "15551234567").
		Str("portal_receiver", "15551234567").
		Str("portal_id", "447700900123@s.whatsapp.net").
		Logger()
	log.Info().
		Str("push_name", "Sam Rivera").
		Str("time", "2026-10-21T00:23:21.142093469Z").
		Str("version", "v0.32.1-0.20261021000255-2cbc91848f4c").
		Int("conversation_count", 278).
		Int64("oldest_msg_ts", 1729000000).
		Str("chat_jid", "120363000000000001@g.us").
		Msg("Successfully paired 15551234567:15@s.whatsapp.net")

	line := out.String()
	for _, private := range []string{"15551234567", "447700900123", "120363000000000001", "Sam Rivera"} {
		if strings.Contains(line, private) {
			t.Errorf("%q is in the log line: %s", private, line)
		}
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(line), &fields); err != nil {
		t.Fatalf("not one JSON line: %v: %s", err, line)
	}
	if fields["login_id"] != fields["portal_receiver"] || !strings.HasPrefix(fields["login_id"].(string), "#") {
		t.Errorf("one number should hash the same everywhere: %v, %v", fields["login_id"], fields["portal_receiver"])
	}
	if !strings.HasPrefix(fields["message"].(string), "Successfully paired "+fields["login_id"].(string)+":15@") {
		t.Errorf("the number in the message should hash like the field's: %v", fields["message"])
	}
	for key, want := range map[string]any{
		"push_name":          "<redacted>",
		"time":               "2026-10-21T00:23:21.142093469Z",
		"version":            "v0.32.1-0.20261021000255-2cbc91848f4c",
		"conversation_count": 278.0,
		"oldest_msg_ts":      1729000000.0,
		"level":              "info",
	} {
		if fields[key] != want {
			t.Errorf("%s = %v, want %v", key, fields[key], want)
		}
	}

	other := newRedactingWriter(&bytes.Buffer{})
	if string(other.redactDigits([]byte("15551234567"))) == fields["login_id"] {
		t.Error("another run of the bridge should hash numbers differently")
	}
}

func TestRedactingWriterNotJSON(t *testing.T) {
	var out bytes.Buffer
	_, _ = newRedactingWriter(&out).Write([]byte("panic: login 15551234567 at 12.3456789s\n"))
	if strings.Contains(out.String(), "15551234567") || !strings.Contains(out.String(), "12.3456789s") {
		t.Errorf("got %q", out.String())
	}
}
