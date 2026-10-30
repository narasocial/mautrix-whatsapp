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
	"cmp"
	"context"
	"maps"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// NaraContentKey is the content key of the block every message and edit event carries for Nara
// (https://nara.contact), whose appservice reads the bridge's events off the homeserver.
// It says whose WhatsApp an event came through, which chat it's in, who sent it and WhatsApp's own
// message ID, none of which a Matrix event otherwise carries. The receiver's own reactions carry one
// too (reaction: true, under NaraReactionKey), so Nara can count one as their answer to the message it's on; other
// reactions and redactions have none: Nara finds them by the event they relate to.
const NaraContentKey = "social.nara"

// NaraReactionKey is the content key of the block on the receiver's own reactions: the same block,
// with reaction: true, under a key of its own, so a Nara server from before it read reactions drops
// one (as an event with no block) rather than taking it for a message of theirs.
const NaraReactionKey = "social.nara.reaction"

// NaraMessageInfo is the social.nara block, version 1. Phone numbers are E.164 with a leading +.
type NaraMessageInfo struct {
	V int `json:"v"`
	// The login the event came through: the human's own number, without the +.
	Receiver string `json:"receiver"`
	// The chat's JID, as in the room's m.bridge state: …@lid or …@s.whatsapp.net for a DM, …@g.us for a group.
	Chat     string `json:"chat"`
	ChatName string `json:"chat_name,omitempty"` // a group's subject
	Direct   bool   `json:"direct"`
	// In a DM, the other person: their number, the receiver's own name for them (from their address
	// book) and the name they go by on WhatsApp (business name, else push name).
	PeerPhone  string `json:"peer_phone,omitempty"`
	PeerName   string `json:"peer_name,omitempty"`
	PeerWAName string `json:"peer_wa_name,omitempty"`
	// WhatsApp's ID for this message, and in an edit, the ID of the message it changes.
	MessageID string `json:"message_id"`
	TargetID  string `json:"target_id,omitempty"`
	// The sender's JID (their LID when it's known), number, the receiver's name for them and their WhatsApp name.
	Sender       string `json:"sender"`
	SenderPhone  string `json:"sender_phone,omitempty"`
	SenderName   string `json:"sender_name,omitempty"`
	SenderWAName string `json:"sender_wa_name,omitempty"`
	FromMe       bool   `json:"from_me"`
	// True for a message backfilled from the history sync, false for one that arrived live.
	History bool `json:"history"`
	// In the second message of a photo or video sent in HD, the first message's ID.
	HDOf string `json:"hd_of,omitempty"`
	// The receiver's own reaction, on the message TargetID: no message of its own.
	Reaction bool `json:"reaction,omitempty"`
}

// naraReactionInfo builds the block for the receiver's own reaction (id) on a message (target) in
// chat, as WhatsApp gives the chat. No names: Nara needs only whose it is and which message it's on.
func naraReactionInfo(receiver networkid.UserLoginID, chat types.JID, id, target types.MessageID, ownLID, ownPN types.JID, history bool) *NaraMessageInfo {
	own := ownLID
	if own.IsEmpty() {
		own = ownPN
	}
	return &NaraMessageInfo{
		V:         1,
		Receiver:  string(receiver),
		Chat:      chat.ToNonAD().String(),
		Direct:    isNaraDirect(chat),
		MessageID: string(id),
		TargetID:  string(target),
		Sender:    own.ToNonAD().String(),
		FromMe:    true,
		History:   history,
		Reaction:  true,
	}
}

// naraMessage is what a NaraMessageInfo is built from.
type naraMessage struct {
	Chat      types.JID
	ChatName  string
	ID        types.MessageID
	Sender    types.JID
	SenderAlt types.JID
	FromMe    bool
	History   bool
	TargetID  types.MessageID
	HDOf      types.MessageID
}

func naraMessageFrom(info *types.MessageInfo, history bool, targetID, hdOf types.MessageID) naraMessage {
	return naraMessage{
		ID:        info.ID,
		Sender:    info.Sender,
		SenderAlt: info.SenderAlt,
		FromMe:    info.IsFromMe,
		History:   history,
		TargetID:  targetID,
		HDOf:      hdOf,
	}
}

// naraStore is the part of the login's whatsmeow store the block reads.
type naraStore interface {
	GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error)
	GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error)
	GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error)
}

type deviceNaraStore struct{ device *store.Device }

func (s deviceNaraStore) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	return s.device.LIDs.GetPNForLID(ctx, lid)
}

func (s deviceNaraStore) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	return s.device.LIDs.GetLIDForPN(ctx, pn)
}

func (s deviceNaraStore) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	return s.device.Contacts.GetContact(ctx, user)
}

// naraInfo builds the block for a message in the portal, or returns nil if it can't.
func (wa *WhatsAppClient) naraInfo(ctx context.Context, portal *bridgev2.Portal, cli *whatsmeow.Client, msg naraMessage) *NaraMessageInfo {
	chat, err := waid.ParsePortalID(portal.ID)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to parse portal ID for the social.nara block")
		return nil
	}
	msg.Chat = chat
	if !isNaraDirect(chat) {
		msg.ChatName = portal.Name
	}
	device := wa.GetStore()
	if cli != nil && cli.Store != nil {
		device = cli.Store
	}
	return buildNaraInfo(ctx, deviceNaraStore{device}, wa.UserLogin.ID, wa.JID, wa.GetLID(), msg)
}

func isNaraDirect(chat types.JID) bool {
	return chat.Server == types.HiddenUserServer || chat.Server == types.DefaultUserServer || chat.Server == types.BotServer
}

func buildNaraInfo(ctx context.Context, st naraStore, receiver networkid.UserLoginID, ownPN, ownLID types.JID, msg naraMessage) *NaraMessageInfo {
	info := &NaraMessageInfo{
		V:         1,
		Receiver:  string(receiver),
		Chat:      msg.Chat.String(),
		ChatName:  msg.ChatName,
		Direct:    isNaraDirect(msg.Chat),
		MessageID: string(msg.ID),
		TargetID:  string(msg.TargetID),
		FromMe:    msg.FromMe,
		History:   msg.History,
		HDOf:      string(msg.HDOf),
	}
	if info.Direct {
		peer := resolveNaraPerson(ctx, st, msg.Chat, types.EmptyJID, true)
		info.PeerPhone, info.PeerName, info.PeerWAName = peer.phone, peer.name, peer.waName
	}
	var sender naraPerson
	if msg.FromMe {
		// The receiver's own messages: their LID and number, no names.
		sender = resolveNaraPerson(ctx, st, ownLID, ownPN, false)
	} else {
		senderJID := msg.Sender
		if senderJID.IsEmpty() && info.Direct {
			senderJID = msg.Chat
		}
		sender = resolveNaraPerson(ctx, st, senderJID, msg.SenderAlt, true)
	}
	info.Sender, info.SenderPhone, info.SenderName, info.SenderWAName = sender.jid.String(), sender.phone, sender.name, sender.waName
	return info
}

type naraPerson struct {
	jid    types.JID // their LID when it's known
	phone  string
	name   string
	waName string
}

// resolveNaraPerson finds a WhatsApp user's LID and number from whichever of the two it has,
// and the names the receiver's store has for them.
func resolveNaraPerson(ctx context.Context, st naraStore, jid, alt types.JID, withNames bool) naraPerson {
	var lid, pn types.JID
	for _, j := range []types.JID{jid.ToNonAD(), alt.ToNonAD()} {
		switch {
		case j.Server == types.HiddenUserServer && lid.IsEmpty():
			lid = j
		case j.Server == types.DefaultUserServer && pn.IsEmpty():
			pn = j
		}
	}
	log := zerolog.Ctx(ctx)
	var err error
	if pn.IsEmpty() && !lid.IsEmpty() {
		if pn, err = st.GetPNForLID(ctx, lid); err != nil {
			log.Warn().Err(err).Stringer("lid", lid).Msg("Failed to get phone number for the social.nara block")
		}
	}
	if lid.IsEmpty() && !pn.IsEmpty() {
		if lid, err = st.GetLIDForPN(ctx, pn); err != nil {
			log.Warn().Err(err).Stringer("pn", pn).Msg("Failed to get LID for the social.nara block")
		}
	}
	person := naraPerson{jid: jid.ToNonAD()}
	if !lid.IsEmpty() {
		person.jid = lid
	} else if person.jid.IsEmpty() {
		person.jid = pn
	}
	if !pn.IsEmpty() {
		person.phone = "+" + pn.User
	}
	if !withNames {
		return person
	}
	// Address book names are kept under phone numbers, push names mostly under LIDs.
	for _, j := range []types.JID{pn, lid} {
		if j.IsEmpty() {
			continue
		}
		contact, err := st.GetContact(ctx, j)
		if err != nil {
			log.Warn().Err(err).Stringer("jid", j).Msg("Failed to get contact for the social.nara block")
			continue
		}
		person.name = cmp.Or(person.name, contact.FullName, contact.FirstName)
		person.waName = cmp.Or(person.waName, contact.BusinessName, contact.PushName)
	}
	return person
}

// hdParentID returns the ID of the message an HD photo or video's second message upgrades.
func hdParentID(msg *waE2E.Message) types.MessageID {
	for _, m := range []*waE2E.Message{msg, msg.GetAssociatedChildMessage().GetMessage()} {
		assoc := m.GetMessageContextInfo().GetMessageAssociation()
		switch assoc.GetAssociationType() {
		case waE2E.MessageAssociation_HD_IMAGE_DUAL_UPLOAD, waE2E.MessageAssociation_HD_VIDEO_DUAL_UPLOAD:
			return assoc.GetParentMessageKey().GetID()
		}
	}
	return ""
}

// setNaraInfo adds the block to each part of a converted message. Parts are copied first,
// since some are shared.
func setNaraInfo(cm *bridgev2.ConvertedMessage, info *NaraMessageInfo) {
	if cm == nil || info == nil {
		return
	}
	for i, part := range cm.Parts {
		withInfo := *part
		withInfo.Extra = maps.Clone(part.Extra)
		if withInfo.Extra == nil {
			withInfo.Extra = make(map[string]any, 1)
		}
		withInfo.Extra[NaraContentKey] = info
		cm.Parts[i] = &withInfo
	}
}

// setNaraEditInfo adds the block to the top level of each part of an edit.
func setNaraEditInfo(edit *bridgev2.ConvertedEdit, info *NaraMessageInfo) {
	if edit == nil {
		return
	}
	for _, part := range edit.ModifiedParts {
		// A caption edit reuses the original message's part, block and all. Only the edit's own goes out.
		if _, ok := part.Extra[NaraContentKey]; ok {
			part.Extra = maps.Clone(part.Extra)
			delete(part.Extra, NaraContentKey)
		}
		if info == nil {
			continue
		}
		if part.TopLevelExtra == nil {
			part.TopLevelExtra = make(map[string]any, 1)
		}
		part.TopLevelExtra[NaraContentKey] = info
	}
}
