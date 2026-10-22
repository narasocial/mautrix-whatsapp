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
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
)

// Whether the human's phone still lists this device, for Nara (https://nara.contact). WhatsApp can
// stop sending a linked device messages without telling it: the phone drops the device from its
// list, and the server goes on authenticating it and routing it receipts, but no one encrypts a
// message for it any more. The bridge would stay CONNECTED with nothing arriving.
//
// The check asks WhatsApp for the account's own devices (the usync query senders make before
// encrypting a message) and looks for this one: in the server's list, and with its key index still
// valid in the list the phone signs (ADVKeyIndexList).

type naraKeyIndexList struct {
	// When the phone signed it, in seconds.
	Timestamp uint64 `json:"ts"`
	// The newest device's key index, and the others still valid.
	Current uint32   `json:"current"`
	Valid   []uint32 `json:"valid"`
	// Signed with the account key this device was linked with (or naming none: it's the account's own list).
	SameAccount bool `json:"same_account"`
}

type naraListedDevice struct {
	ID       uint16  `json:"id"`
	KeyIndex *uint32 `json:"key_index,omitempty"`
	Hosted   bool    `json:"hosted,omitempty"`
}

type naraDeviceList struct {
	Devices    []naraListedDevice `json:"devices"`
	KeyIndexes *naraKeyIndexList  `json:"key_indexes,omitempty"`
}

type naraDeviceCheck struct {
	// This device, and what its link says.
	Device   uint16          `json:"device"`
	KeyIndex uint32          `json:"key_index"`
	LinkedAt uint64          `json:"linked_at"`
	PN       *naraDeviceList `json:"pn,omitempty"`
	LID      *naraDeviceList `json:"lid,omitempty"`
	LIDError string          `json:"lid_error,omitempty"`
}

// naraParseDevices reads a usync <devices> node: the server's list of the account's devices and the
// phone's signed key indexes, when it sent them. accountKey is the account signature key this
// device was linked with.
func naraParseDevices(node waBinary.Node, accountKey []byte) (*naraDeviceList, error) {
	if node.Tag != "devices" {
		return nil, fmt.Errorf("no devices node")
	}
	deviceList, ok := node.GetOptionalChildByTag("device-list")
	if !ok {
		return nil, fmt.Errorf("no device-list node")
	}
	list := &naraDeviceList{Devices: []naraListedDevice{}}
	for _, child := range deviceList.GetChildren() {
		ag := child.AttrGetter()
		id, ok := ag.GetInt64("id", true)
		if child.Tag != "device" || !ok {
			continue
		}
		device := naraListedDevice{ID: uint16(id), Hosted: ag.OptionalBool("is_hosted")}
		if keyIndex, ok := ag.GetInt64("key-index", false); ok {
			k := uint32(keyIndex)
			device.KeyIndex = &k
		}
		list.Devices = append(list.Devices, device)
	}
	if keyIndexNode, ok := node.GetOptionalChildByTag("key-index-list"); ok {
		raw, ok := keyIndexNode.Content.([]byte)
		if !ok {
			return nil, fmt.Errorf("key-index-list has no content")
		}
		var signed waAdv.ADVSignedKeyIndexList
		if err := proto.Unmarshal(raw, &signed); err != nil {
			return nil, fmt.Errorf("failed to parse signed key index list: %w", err)
		}
		var details waAdv.ADVKeyIndexList
		if err := proto.Unmarshal(signed.GetDetails(), &details); err != nil {
			return nil, fmt.Errorf("failed to parse key index list: %w", err)
		}
		list.KeyIndexes = &naraKeyIndexList{
			Timestamp:   details.GetTimestamp(),
			Current:     details.GetCurrentIndex(),
			Valid:       slices.Clone(details.GetValidIndexes()),
			SameAccount: len(signed.GetAccountSignatureKey()) == 0 || slices.Equal(signed.GetAccountSignatureKey(), accountKey),
		}
	}
	return list, nil
}

// naraQueryDevices asks WhatsApp for the devices of one of the account's own JIDs, bypassing
// whatsmeow's cache.
func (wa *WhatsAppClient) naraQueryDevices(ctx context.Context, jid types.JID, accountKey []byte) (*naraDeviceList, error) {
	//lint:ignore SA1019 the device list's raw node is needed for its key indexes
	list, err := wa.Client.DangerousInternals().Usync(ctx, []types.JID{jid.ToNonAD()}, "query", "message", []waBinary.Node{
		{Tag: "devices", Attrs: waBinary.Attrs{"version": "2"}},
	})
	if err != nil {
		return nil, err
	}
	for _, user := range list.GetChildren() {
		if user.Tag != "user" {
			continue
		}
		return naraParseDevices(user.GetChildByTag("devices"), accountKey)
	}
	return nil, fmt.Errorf("no user in the usync response")
}

func (wa *WhatsAppClient) naraCheckDevices(ctx context.Context) (*naraDeviceCheck, error) {
	cli := wa.Client
	if cli == nil || !cli.IsLoggedIn() {
		return nil, fmt.Errorf("not connected")
	}
	device := cli.Store
	check := &naraDeviceCheck{Device: device.GetJID().Device}
	var accountKey []byte
	if account := device.Account; account != nil {
		accountKey = account.GetAccountSignatureKey()
		var identity waAdv.ADVDeviceIdentity
		if err := proto.Unmarshal(account.GetDetails(), &identity); err != nil {
			return nil, fmt.Errorf("failed to parse own device identity: %w", err)
		}
		check.KeyIndex = identity.GetKeyIndex()
		check.LinkedAt = identity.GetTimestamp()
	}
	var err error
	check.PN, err = wa.naraQueryDevices(ctx, device.GetJID(), accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get own devices: %w", err)
	}
	if lid := device.GetLID(); !lid.IsEmpty() {
		check.LID, err = wa.naraQueryDevices(ctx, lid, accountKey)
		if err != nil {
			check.LIDError = err.Error()
		}
	}
	return check, nil
}

func (wa *WhatsAppConnector) registerNaraDevices() {
	prov, ok := wa.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return
	}
	api := prov.GetProvisioning()
	api.GetRouter().HandleFunc("GET /v3/nara/devices/{loginID}", func(w http.ResponseWriter, r *http.Request) {
		user := api.GetUser(r)
		login := wa.Bridge.GetCachedUserLoginByID(networkid.UserLoginID(r.PathValue("loginID")))
		if login == nil || user == nil || login.UserMXID != user.MXID {
			mautrix.MNotFound.WithMessage("Login not found").Write(w)
			return
		}
		client, ok := login.Client.(*WhatsAppClient)
		if !ok {
			mautrix.MNotFound.WithMessage("Login not found").Write(w)
			return
		}
		check, err := client.naraCheckDevices(r.Context())
		if err != nil {
			zerolog.Ctx(r.Context()).Err(err).Msg("Failed to check own devices for Nara")
			mautrix.MUnknown.WithMessage(err.Error()).Write(w)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, check)
	})
}

// WADeviceUnlisted is the bad credentials error for a device the account no longer lists.
const WADeviceUnlisted status.BridgeStateErrorCode = "wa-device-unlisted"

const (
	// The first check, once a connection has settled, then every NaraDeviceCheckInterval.
	NaraDeviceCheckDelay    = 2 * time.Minute
	NaraDeviceCheckInterval = time.Hour
	// A device found missing is checked again this long after, and only reported if it's still missing.
	NaraDeviceConfirmDelay = 5 * time.Minute
)

// unlisted says why the account no longer lists this device, or "" while it does: it's missing from
// the server's list of the account's devices, or its key index isn't valid in the phone's signed
// list, when that list is signed with this device's account key and isn't older than its link.
func (check *naraDeviceCheck) unlisted() string {
	if check.PN == nil {
		return ""
	}
	if !slices.ContainsFunc(check.PN.Devices, func(d naraListedDevice) bool { return d.ID == check.Device }) {
		return "not in the account's device list"
	}
	keys := check.PN.KeyIndexes
	if keys == nil || !keys.SameAccount || keys.Timestamp < check.LinkedAt || check.KeyIndex == 0 {
		return ""
	}
	if keys.Current != check.KeyIndex && !slices.Contains(keys.Valid, check.KeyIndex) {
		return "key index not valid in the phone's list"
	}
	return ""
}

// naraDeviceCheckLoop checks, while the login is connected, that the account still lists this
// device, and reports BAD_CREDENTIALS (wa-device-unlisted) once two checks NaraDeviceConfirmDelay
// apart agree it doesn't. The client stays connected, so the logout Nara then asks for still tells
// WhatsApp to remove the device and deletes its keys.
func (wa *WhatsAppClient) naraDeviceCheckLoop(ctx context.Context) {
	log := zerolog.Ctx(ctx).With().Str("action", "nara device check").Logger()
	wait := NaraDeviceCheckDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = NaraDeviceCheckInterval
		if wa.naraUnlisted.Load() {
			return
		}
		reason, ok := wa.naraDeviceMissing(ctx, log)
		if !ok || reason == "" {
			continue
		}
		log.Warn().Str("reason", reason).Msg("Own device missing from the account's devices, checking again")
		select {
		case <-ctx.Done():
			return
		case <-time.After(NaraDeviceConfirmDelay):
		}
		reason, ok = wa.naraDeviceMissing(ctx, log)
		if !ok || reason == "" {
			log.Info().Msg("Own device listed again")
			continue
		}
		log.Warn().Str("reason", reason).Msg("Own device still missing from the account's devices, reporting bad credentials")
		wa.naraUnlisted.Store(true)
		wa.UserLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      WADeviceUnlisted,
			Message:    "Your phone no longer lists this device. Link it again.",
		})
		return
	}
}

// naraDeviceMissing runs one check: why the device is missing ("" when listed), and whether the check ran.
func (wa *WhatsAppClient) naraDeviceMissing(ctx context.Context, log zerolog.Logger) (string, bool) {
	checkCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	check, err := wa.naraCheckDevices(checkCtx)
	if err != nil {
		log.Debug().Err(err).Msg("Couldn't check own devices")
		return "", false
	}
	return check.unlisted(), true
}
