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
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

type fakeNaraStore struct {
	pnByLID  map[types.JID]types.JID
	contacts map[types.JID]types.ContactInfo
}

func (s fakeNaraStore) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	return s.pnByLID[lid], nil
}

func (s fakeNaraStore) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	for lid, p := range s.pnByLID {
		if p == pn {
			return lid, nil
		}
	}
	return types.EmptyJID, nil
}

func (s fakeNaraStore) GetContact(_ context.Context, user types.JID) (types.ContactInfo, error) {
	contact, ok := s.contacts[user]
	contact.Found = ok
	return contact, nil
}

var (
	ownPN     = types.NewJID("15550000001", types.DefaultUserServer)
	ownLID    = types.NewJID("100000000000001", types.HiddenUserServer)
	friendPN  = types.NewJID("447700900123", types.DefaultUserServer)
	friendLID = types.NewJID("200000000000002", types.HiddenUserServer)
	otherLID  = types.NewJID("300000000000003", types.HiddenUserServer)
	groupJID  = types.NewJID("120363000000000001", types.GroupServer)

	testNaraStore = fakeNaraStore{
		pnByLID: map[types.JID]types.JID{ownLID: ownPN, friendLID: friendPN},
		contacts: map[types.JID]types.ContactInfo{
			// The receiver's address book is kept by phone number, push names by LID.
			friendPN:  {FullName: "Sam Rivera", FirstName: "Sam"},
			friendLID: {PushName: "sammy"},
			otherLID:  {PushName: "Alex", BusinessName: "Alex's Bakery"},
		},
	}
)

func TestBuildNaraInfo(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		msg  naraMessage
		want NaraMessageInfo
	}{{
		name: "live DM from the other person",
		msg:  naraMessage{Chat: friendLID, ID: "3EB0A1", Sender: friendLID},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "200000000000002@lid", Direct: true,
			PeerPhone: "+447700900123", PeerName: "Sam Rivera", PeerWAName: "sammy",
			MessageID: "3EB0A1", Sender: "200000000000002@lid",
			SenderPhone: "+447700900123", SenderName: "Sam Rivera", SenderWAName: "sammy",
		},
	}, {
		name: "the receiver's own message in a group, from history",
		msg: naraMessage{
			Chat: groupJID, ChatName: "Climbing", ID: "3EB0A2",
			Sender: types.NewADJID(ownPN.User, 0, 12), FromMe: true, History: true,
		},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "120363000000000001@g.us", ChatName: "Climbing",
			MessageID: "3EB0A2", Sender: "100000000000001@lid", SenderPhone: "+15550000001",
			FromMe: true, History: true,
		},
	}, {
		name: "group sender by phone number, from one of their devices",
		msg:  naraMessage{Chat: groupJID, ID: "3EB0A3", Sender: types.NewADJID(friendPN.User, 0, 3)},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "120363000000000001@g.us",
			MessageID: "3EB0A3", Sender: "200000000000002@lid",
			SenderPhone: "+447700900123", SenderName: "Sam Rivera", SenderWAName: "sammy",
		},
	}, {
		name: "group sender whose number isn't known",
		msg:  naraMessage{Chat: groupJID, ID: "3EB0A4", Sender: otherLID},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "120363000000000001@g.us",
			MessageID: "3EB0A4", Sender: "300000000000003@lid", SenderWAName: "Alex's Bakery",
		},
	}, {
		name: "an HD photo's second message, as an edit of its first",
		msg:  naraMessage{Chat: friendLID, ID: "3EB0A6", Sender: friendLID, SenderAlt: friendPN, TargetID: "3EB0A5", HDOf: "3EB0A5"},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "200000000000002@lid", Direct: true,
			PeerPhone: "+447700900123", PeerName: "Sam Rivera", PeerWAName: "sammy",
			MessageID: "3EB0A6", TargetID: "3EB0A5", HDOf: "3EB0A5", Sender: "200000000000002@lid",
			SenderPhone: "+447700900123", SenderName: "Sam Rivera", SenderWAName: "sammy",
		},
	}, {
		name: "DM by phone number from history, with no sender",
		msg:  naraMessage{Chat: friendPN, ID: "3EB0A7", History: true},
		want: NaraMessageInfo{
			V: 1, Receiver: "15550000001", Chat: "447700900123@s.whatsapp.net", Direct: true,
			PeerPhone: "+447700900123", PeerName: "Sam Rivera", PeerWAName: "sammy",
			MessageID: "3EB0A7", Sender: "200000000000002@lid",
			SenderPhone: "+447700900123", SenderName: "Sam Rivera", SenderWAName: "sammy", History: true,
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := buildNaraInfo(ctx, testNaraStore, "15550000001", types.NewADJID(ownPN.User, 0, 12), ownLID, tc.msg)
			if !reflect.DeepEqual(*got, tc.want) {
				t.Errorf("got  %+v\nwant %+v", *got, tc.want)
			}
		})
	}
}

func TestNaraInfoJSON(t *testing.T) {
	info := buildNaraInfo(context.Background(), testNaraStore, "15550000001", ownPN, ownLID, naraMessage{Chat: groupJID, ID: "3EB0A4", Sender: otherLID})
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	// Values that aren't known are left out, except the flags, which are always there.
	want := []string{"chat", "direct", "from_me", "history", "message_id", "receiver", "sender", "sender_wa_name", "v"}
	if !slices.Equal(keys, want) {
		t.Errorf("got keys %v, want %v", keys, want)
	}
}

func TestSetNaraInfo(t *testing.T) {
	info := &NaraMessageInfo{V: 1, MessageID: "3EB0A1"}
	shared := &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "hi"},
		Extra:   map[string]any{"fi.mau.whatsapp.source_broadcast_list": "1@broadcast"},
	}
	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{shared}}
	setNaraInfo(cm, info)

	if cm.Parts[0] == shared {
		t.Fatal("the part wasn't copied")
	}
	if cm.Parts[0].Extra[NaraContentKey] != info || cm.Parts[0].Extra["fi.mau.whatsapp.source_broadcast_list"] != "1@broadcast" {
		t.Errorf("got extra %v", cm.Parts[0].Extra)
	}
	if _, ok := shared.Extra[NaraContentKey]; ok {
		t.Error("the shared part was changed")
	}

	setNaraInfo(cm, nil)
	if cm.Parts[0].Extra[NaraContentKey] != info {
		t.Error("a nil block replaced the block")
	}
	setNaraInfo(nil, info)
}

func TestSetNaraEditInfo(t *testing.T) {
	original := &NaraMessageInfo{V: 1, MessageID: "3EB0A1"}
	edit := &NaraMessageInfo{V: 1, MessageID: "3EB0A9", TargetID: "3EB0A1"}
	// A caption edit reuses the original message's part, and with it its extra.
	cachedExtra := map[string]any{NaraContentKey: original, "info": map[string]any{"mimetype": "image/jpeg"}}
	converted := &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{
		Type:          event.EventMessage,
		Content:       &event.MessageEventContent{MsgType: event.MsgImage, Body: "new caption"},
		Extra:         cachedExtra,
		TopLevelExtra: map[string]any{"com.beeper.dont_render_edited": true},
	}}}
	setNaraEditInfo(converted, edit)

	part := converted.ModifiedParts[0]
	if part.TopLevelExtra[NaraContentKey] != edit || part.TopLevelExtra["com.beeper.dont_render_edited"] != true {
		t.Errorf("got top level extra %v", part.TopLevelExtra)
	}
	if _, ok := part.Extra[NaraContentKey]; ok || part.Extra["info"] == nil {
		t.Errorf("got new content extra %v", part.Extra)
	}
	if cachedExtra[NaraContentKey] != original {
		t.Error("the cached part's extra was changed")
	}
	setNaraEditInfo(nil, edit)
}

func TestHDParentID(t *testing.T) {
	hd := func(assocType waE2E.MessageAssociation_AssociationType) *waE2E.MessageContextInfo {
		return &waE2E.MessageContextInfo{MessageAssociation: &waE2E.MessageAssociation{
			AssociationType:  assocType.Enum(),
			ParentMessageKey: &waCommon.MessageKey{ID: proto.String("3EB0A5")},
		}}
	}
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}
	for _, tc := range []struct {
		name string
		msg  *waE2E.Message
		want types.MessageID
	}{
		{"HD image", &waE2E.Message{
			MessageContextInfo:     hd(waE2E.MessageAssociation_HD_IMAGE_DUAL_UPLOAD),
			AssociatedChildMessage: &waE2E.FutureProofMessage{Message: image},
		}, "3EB0A5"},
		{"HD video, marked on the child", &waE2E.Message{AssociatedChildMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
			MessageContextInfo: hd(waE2E.MessageAssociation_HD_VIDEO_DUAL_UPLOAD),
			VideoMessage:       &waE2E.VideoMessage{},
		}}}, "3EB0A5"},
		{"motion photo", &waE2E.Message{MessageContextInfo: hd(waE2E.MessageAssociation_MOTION_PHOTO)}, ""},
		{"plain photo", image, ""},
		{"nil", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hdParentID(tc.msg); got != tc.want {
				t.Errorf("hdParentID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNaraReactionInfo(t *testing.T) {
	chat := types.NewJID("200000000000002", types.HiddenUserServer)
	ownLID := types.NewJID("100000000000001", types.HiddenUserServer)
	ownPN := types.NewJID("15550000001", types.DefaultUserServer)
	info := naraReactionInfo("15550000001", chat, "3EB0REACT", "3EB0TARGET", ownLID, ownPN, false)
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"receiver":"15550000001","chat":"200000000000002@lid","direct":true,"message_id":"3EB0REACT","target_id":"3EB0TARGET","sender":"100000000000001@lid","from_me":true,"history":false,"reaction":true}`
	if string(raw) != want {
		t.Fatalf("got %s, want %s", raw, want)
	}
	// No LID known yet: their number stands in.
	if got := naraReactionInfo("15550000001", chat, "a", "b", types.EmptyJID, ownPN, true).Sender; got != "15550000001@s.whatsapp.net" {
		t.Fatalf("sender %s", got)
	}
}
