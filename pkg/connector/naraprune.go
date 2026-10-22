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
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Keeping no history sync message once nothing will read it, for Nara (https://nara.contact).
//
// The history sync's messages wait in whatsapp_history_sync_message, each the raw protobuf with its
// text, until their chat's portal is made and backfilled, and the backfill deletes them once it's
// sent. Without the backfill queue, that's the only read, and some are never deleted by it: they stay
// until the login logs out.
//   - A chat whose portal was already there (a relogin that kept the login, or a live message that
//     made it first): the resync backfills nothing older than what the portal has.
//   - A chat whose backfill failed, or whose portal couldn't be made.
//   - A chat that never gets a portal (a PSA chat, one whose info couldn't be fetched).
//   - History that comes for a chat after its portal was made in this login: the next portal
//     creation skips the chat (it's synced), and nothing else reads it.
//
// So while the backfill queue is off, every NaraPruneInterval and at start the bridge deletes the
// messages of every chat bridged since its login logged in (the chats portal creation skips), and
// every message the history sync stored more than NaraHistoryMaxAge ago, which nothing waits that
// long to read (wait_for_full_sync, 3 h at most, then the backfills).

// NaraPruneInterval is how often the bridge deletes history sync messages nothing will read.
const NaraPruneInterval = 10 * time.Minute

// NaraHistoryMaxAge is how long a history sync message is kept at most.
const NaraHistoryMaxAge = 24 * time.Hour

func (wa *WhatsAppConnector) naraPruneLoop() {
	ctx := wa.Bridge.BackgroundCtx
	log := wa.Bridge.Log.With().Str("component", "nara_prune").Logger()
	if wa.Bridge.Config.Backfill.Queue.AnyEnabled() {
		log.Debug().Msg("Not pruning history sync messages, as the backfill queue reads them")
		return
	} else if wa.Bridge.DB.Dialect != dbutil.Postgres {
		log.Warn().Msg("Not pruning history sync messages, which needs Postgres")
		return
	}
	ticker := time.NewTicker(NaraPruneInterval)
	defer ticker.Stop()
	for {
		synced, old, err := naraPruneHistory(ctx, wa.Bridge.DB.Database, wa.Bridge.ID, time.Now())
		if err != nil {
			log.Err(err).Msg("Failed to prune history sync messages")
		} else if synced > 0 || old > 0 {
			log.Info().
				Int64("synced_chats_messages", synced).
				Int64("old_messages", old).
				Msg("Pruned history sync messages")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// naraPruneHistory deletes the history sync messages of the chats that portal creation now skips,
// those synced at or after their login's login time (as GetRecent selects them), and every one
// stored more than NaraHistoryMaxAge before now. It says how many of each it deleted.
func naraPruneHistory(ctx context.Context, db *dbutil.Database, bridgeID networkid.BridgeID, now time.Time) (synced, old int64, err error) {
	res, err := db.Exec(ctx, `
		DELETE FROM whatsapp_history_sync_message m
		USING whatsapp_history_sync_conversation c, user_login ul
		WHERE m.bridge_id = $1
		  AND c.bridge_id = m.bridge_id AND c.user_login_id = m.user_login_id AND c.chat_jid = m.chat_jid
		  AND ul.bridge_id = m.bridge_id AND ul.id = m.user_login_id
		  AND c.synced_login_ts >= COALESCE((ul.metadata->>'logged_in_at')::bigint, 0)
	`, bridgeID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to delete synced chats' history sync messages: %w", err)
	}
	synced, _ = res.RowsAffected()
	res, err = db.Exec(ctx, `
		DELETE FROM whatsapp_history_sync_message WHERE bridge_id = $1 AND inserted_time < $2
	`, bridgeID, now.Add(-NaraHistoryMaxAge).Unix())
	if err != nil {
		return synced, 0, fmt.Errorf("failed to delete old history sync messages: %w", err)
	}
	old, _ = res.RowsAffected()
	return synced, old, nil
}
