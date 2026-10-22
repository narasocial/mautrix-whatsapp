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
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/wadb"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

// NARA_TEST_POSTGRES is a Postgres URI the test may make a schema in (nara_prune_test), e.g.
// postgres://user:password@localhost/db?sslmode=disable. Without it, the test is skipped.
func naraPruneTestDB(t *testing.T) *dbutil.Database {
	t.Helper()
	uri := os.Getenv("NARA_TEST_POSTGRES")
	if uri == "" {
		t.Skip("NARA_TEST_POSTGRES not set")
	}
	ctx := context.Background()
	admin, err := dbutil.NewWithDialect(uri, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "DROP SCHEMA IF EXISTS nara_prune_test CASCADE; CREATE SCHEMA nara_prune_test"); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close()
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	db, err := dbutil.NewWithDialect(uri+sep+"search_path=nara_prune_test", "postgres")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestNaraPruneHistory(t *testing.T) {
	ctx := context.Background()
	db := naraPruneTestDB(t)
	const bridgeID = networkid.BridgeID("whatsapp")
	bridgeDB := database.New(bridgeID, database.MetaTypes{
		UserLogin: func() any { return &waid.UserLoginMetadata{} },
	}, db)
	if err := bridgeDB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	waDB := wadb.New(bridgeID, db, zerolog.Nop())
	if err := waDB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	loggedIn := jsontime.U(now.Add(-time.Hour))
	before := jsontime.U(now.Add(-48 * time.Hour))
	if _, err := db.Exec(ctx, `INSERT INTO "user" (bridge_id, mxid) VALUES ($1, '@h:nara.localhost')`, bridgeID); err != nil {
		t.Fatal(err)
	}
	logins := map[networkid.UserLoginID]jsontime.Unix{"15550000001": loggedIn, "15550000002": {}}
	for loginID, loginTS := range logins {
		meta, err := json.Marshal(&waid.UserLoginMetadata{LoggedInAt: loginTS})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(ctx, `
			INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, metadata) VALUES ($1, '@h:nara.localhost', $2, '', $3)
		`, bridgeID, loginID, meta); err != nil {
			t.Fatal(err)
		}
	}

	put := func(loginID networkid.UserLoginID, chat string, ids ...string) types.JID {
		t.Helper()
		jid := types.NewJID(chat, types.DefaultUserServer)
		if err := waDB.Conversation.Put(ctx, &wadb.Conversation{UserLoginID: loginID, ChatJID: jid, LastMessageTimestamp: now}); err != nil {
			t.Fatal(err)
		}
		var msgs []*wadb.HistorySyncMessageTuple
		for _, id := range ids {
			msgs = append(msgs, &wadb.HistorySyncMessageTuple{
				Info:    &types.MessageInfo{MessageSource: types.MessageSource{Sender: jid}, ID: id, Timestamp: now},
				Message: []byte("words"),
			})
		}
		if err := waDB.Message.Put(ctx, loginID, jid, msgs); err != nil {
			t.Fatal(err)
		}
		return jid
	}
	// Bridged in this login, and portal creation won't pick it again: its messages go.
	synced := put("15550000001", "100", "a", "b")
	if err := waDB.Conversation.MarkSynced(ctx, "15550000001", synced, loggedIn); err != nil {
		t.Fatal(err)
	}
	// Bridged in an earlier login of the same account: the next portal creation resyncs it, so it stays.
	resync := put("15550000001", "101", "c")
	if err := waDB.Conversation.MarkSynced(ctx, "15550000001", resync, before); err != nil {
		t.Fatal(err)
	}
	// Waiting for its portal: it stays.
	waiting := put("15550000001", "102", "d")
	// Waiting a day: it goes.
	stale := put("15550000001", "103", "e")
	if _, err := db.Exec(ctx, `UPDATE whatsapp_history_sync_message SET inserted_time = $1 WHERE chat_jid = $2`,
		now.Add(-NaraHistoryMaxAge-time.Minute).Unix(), stale); err != nil {
		t.Fatal(err)
	}
	// A login from before logged_in_at: anything it synced is never picked again.
	oldLogin := put("15550000002", "100", "f")
	if err := waDB.Conversation.MarkSynced(ctx, "15550000002", oldLogin, before); err != nil {
		t.Fatal(err)
	}
	// Another bridge's rows are never touched.
	if _, err := db.Exec(ctx, `
		INSERT INTO "user" (bridge_id, mxid) VALUES ('other', '@h:nara.localhost');
		INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, metadata) VALUES ('other', '@h:nara.localhost', '15550000001', '', '{}');
		INSERT INTO whatsapp_history_sync_conversation (bridge_id, user_login_id, chat_jid, synced_login_ts) VALUES ('other', '15550000001', '100@s.whatsapp.net', 1);
		INSERT INTO whatsapp_history_sync_message (bridge_id, user_login_id, chat_jid, sender_jid, message_id, timestamp, data, inserted_time)
		VALUES ('other', '15550000001', '100@s.whatsapp.net', '100@s.whatsapp.net', 'g', 1, 'words', 1)
	`); err != nil {
		t.Fatal(err)
	}

	gotSynced, gotOld, err := naraPruneHistory(ctx, db, bridgeID, now)
	if err != nil {
		t.Fatal(err)
	}
	if gotSynced != 3 || gotOld != 1 {
		t.Errorf("pruned %d synced chats' messages and %d old ones, want 3 and 1", gotSynced, gotOld)
	}
	left := func(loginID networkid.UserLoginID, jid types.JID) (n int) {
		t.Helper()
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE bridge_id = $1 AND user_login_id = $2 AND chat_jid = $3
		`, bridgeID, loginID, jid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, c := range []struct {
		name  string
		login networkid.UserLoginID
		jid   types.JID
		want  int
	}{
		{"synced in this login", "15550000001", synced, 0},
		{"synced in an earlier login", "15550000001", resync, 1},
		{"waiting for its portal", "15550000001", waiting, 1},
		{"stored a day ago", "15550000001", stale, 0},
		{"synced by a login without logged_in_at", "15550000002", oldLogin, 0},
	} {
		if got := left(c.login, c.jid); got != c.want {
			t.Errorf("%s: %d messages left, want %d", c.name, got, c.want)
		}
	}
	var otherLeft int
	if err = db.QueryRow(ctx, `SELECT COUNT(*) FROM whatsapp_history_sync_message WHERE bridge_id = 'other'`).Scan(&otherLeft); err != nil {
		t.Fatal(err)
	}
	if otherLeft != 1 {
		t.Errorf("another bridge's messages: %d left, want 1", otherLeft)
	}

	// What it deleted for being synced is exactly what portal creation would skip.
	recent, err := waDB.Conversation.GetRecent(ctx, "15550000001", -1, loggedIn)
	if err != nil {
		t.Fatal(err)
	}
	var recentJIDs []types.JID
	for _, conv := range recent {
		recentJIDs = append(recentJIDs, conv.ChatJID)
	}
	slices.SortFunc(recentJIDs, func(a, b types.JID) int { return strings.Compare(a.String(), b.String()) })
	if want := []types.JID{resync, waiting, stale}; !slices.Equal(recentJIDs, want) {
		t.Errorf("portal creation would pick %v, want %v", recentJIDs, want)
	}
}
