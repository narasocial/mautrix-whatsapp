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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strings"

	"go.mau.fi/zeroconfig"
)

// The redacted-stdout log writer is stdout without the people in it, for a bridge whose logs are
// kept by a hosting provider. Login IDs, portal IDs and JIDs are phone numbers, and so are on
// nearly every line about a login or a chat. In each JSON line, a run of 7 or more digits becomes
// a keyed hash that's the same for the whole run of the bridge (so a login can still be followed
// through the logs) but can't be turned back into the number, and any name or message text is
// dropped.
const redactedStdout zeroconfig.WriterType = "redacted-stdout"

func init() {
	zeroconfig.RegisterWriter(redactedStdout, func(_ *zeroconfig.WriterConfig) (io.Writer, error) {
		return newRedactingWriter(os.Stdout), nil
	})
}

// Digits after a '.' are the fraction of a time or a version, not a number of anyone's.
var longDigits = regexp.MustCompile(`(^|[^0-9.])\+?([0-9]{7,})`)

type redactingWriter struct {
	out io.Writer
	key []byte
}

func newRedactingWriter(out io.Writer) *redactingWriter {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return &redactingWriter{out: out, key: key}
}

func (w *redactingWriter) Write(p []byte) (int, error) {
	var fields map[string]any
	dec := json.NewDecoder(bytes.NewReader(p))
	dec.UseNumber()
	if dec.Decode(&fields) != nil {
		_, err := w.out.Write(w.redactDigits(p))
		return len(p), err
	}
	out, err := json.Marshal(w.redact("", fields))
	if err != nil {
		return 0, err
	}
	_, err = w.out.Write(append(out, '\n'))
	return len(p), err
}

func (w *redactingWriter) redact(key string, value any) any {
	switch v := value.(type) {
	case string:
		if v != "" && isPrivateField(key) {
			return "<redacted>"
		}
		return string(w.redactDigits([]byte(v)))
	case map[string]any:
		for k, item := range v {
			v[k] = w.redact(k, item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = w.redact(key, item)
		}
		return v
	default:
		return v
	}
}

func isPrivateField(key string) bool {
	key = strings.ToLower(key)
	return strings.Contains(key, "name") || key == "body" || key == "text" || key == "caption" || key == "content"
}

func (w *redactingWriter) redactDigits(p []byte) []byte {
	return longDigits.ReplaceAllFunc(p, func(match []byte) []byte {
		groups := longDigits.FindSubmatch(match)
		mac := hmac.New(sha256.New, w.key)
		mac.Write(groups[2])
		return append(groups[1], "#"+hex.EncodeToString(mac.Sum(nil))[:8]...)
	})
}
