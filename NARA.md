# Nara's mautrix-whatsapp

This is the bridge [Nara](https://nara.contact) runs for testers who connect WhatsApp: [mautrix-whatsapp](https://github.com/mautrix/whatsapp) with a few patches, on the `nara` branch. It's published here under the AGPL-3.0, like the original.

The branch starts from upstream's v26.10 tag and adds:

- **Waiting for the full history sync** (`history_sync.wait_for_full_sync`). Portals aren't created after a login until the phone has sent the whole full history sync (a FULL payload with progress 100), or until that long after login. History that arrives after a chat's portal exists is otherwise lost on a homeserver without batch sending. Login bridge states say `info.history: "started"` (with `history_progress`) until every chat's portal and backfill are done, then `"done"`.
- **The `social.nara` block** on every message and edit event: whose WhatsApp it came through, the chat, the sender's LID and phone number, the receiver's own contact name for them, WhatsApp's message ID, and whether it came from history. See `pkg/connector/nara.go`.
- **A `redacted-stdout` log writer** (`cmd/mautrix-whatsapp/redactlog.go`): stdout with every phone number (any run of 7+ digits) replaced by a hash keyed per run, and names and message text dropped, for logs kept by a hosting provider.
- **[narasocial/mautrix-go](https://github.com/narasocial/mautrix-go)'s `nara` branch** in place of mautrix-go (`replace` in `go.mod`): a backfill skips a message the homeserver refuses (like one over the 64 KiB event limit) instead of ending the chat's backfill.

Nara builds its image from a pinned commit of this branch, the way upstream's Dockerfile does.
