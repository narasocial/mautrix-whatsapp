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

package connector

import (
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"google.golang.org/protobuf/proto"
)

var naraAccountKey = []byte("account key")

func naraDevicesNode(t *testing.T, devices []waBinary.Node, keys *waAdv.ADVKeyIndexList, accountKey []byte) waBinary.Node {
	t.Helper()
	content := []waBinary.Node{{Tag: "device-list", Content: devices}}
	if keys != nil {
		details, err := proto.Marshal(keys)
		if err != nil {
			t.Fatal(err)
		}
		signed, err := proto.Marshal(&waAdv.ADVSignedKeyIndexList{Details: details, AccountSignatureKey: accountKey})
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, waBinary.Node{Tag: "key-index-list", Attrs: waBinary.Attrs{"ts": "1792599400"}, Content: signed})
	}
	return waBinary.Node{Tag: "devices", Content: content}
}

func naraDevice(id string, keyIndex string) waBinary.Node {
	attrs := waBinary.Attrs{"id": id}
	if keyIndex != "" {
		attrs["key-index"] = keyIndex
	}
	return waBinary.Node{Tag: "device", Attrs: attrs}
}

func TestNaraDeviceUnlisted(t *testing.T) {
	keys := func(ts uint64, current uint32, valid ...uint32) *waAdv.ADVKeyIndexList {
		return &waAdv.ADVKeyIndexList{Timestamp: proto.Uint64(ts), CurrentIndex: proto.Uint32(current), ValidIndexes: valid}
	}
	for _, tc := range []struct {
		name       string
		devices    []waBinary.Node
		keys       *waAdv.ADVKeyIndexList
		accountKey []byte
		want       string
	}{
		{"listed, valid", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599400, 3, 2), naraAccountKey, ""},
		{"listed, the newest", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599400, 2), naraAccountKey, ""},
		{"listed, no key index list", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, nil, nil, ""},
		{"not in the device list", []waBinary.Node{naraDevice("0", ""), naraDevice("7", "3")}, keys(1792599400, 3, 2, 3), naraAccountKey, "not in the account's device list"},
		{"key index dropped", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599400, 3, 3), naraAccountKey, "key index not valid in the phone's list"},
		{"a list older than the link", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599000, 1, 1), naraAccountKey, ""},
		{"another account's list", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599400, 3, 3), []byte("other"), ""},
		{"a list naming no key", []waBinary.Node{naraDevice("0", ""), naraDevice("6", "2")}, keys(1792599400, 3, 3), nil, "key index not valid in the phone's list"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list, err := naraParseDevices(naraDevicesNode(t, tc.devices, tc.keys, tc.accountKey), naraAccountKey)
			if err != nil {
				t.Fatal(err)
			}
			check := &naraDeviceCheck{Device: 6, KeyIndex: 2, LinkedAt: 1792599304, PN: list}
			if got := check.unlisted(); got != tc.want {
				t.Errorf("unlisted() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNaraParseDevicesRefusesOtherNodes(t *testing.T) {
	if _, err := naraParseDevices(waBinary.Node{Tag: "error"}, naraAccountKey); err == nil {
		t.Error("parsed a node that isn't devices")
	}
}
