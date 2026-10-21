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
	"sync"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/status"

	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

func TestFullSyncWait(t *testing.T) {
	loggedIn := time.Date(2026, 10, 20, 20, 40, 0, 0, time.UTC)
	waiting := func() *waid.UserLoginMetadata {
		return &waid.UserLoginMetadata{
			LoggedInAt:                     jsontime.U(loggedIn),
			HistorySyncPortalsNeedCreating: true,
			HistorySyncFullProgress:        65,
		}
	}
	const maxWait = 3 * time.Hour

	for _, tc := range []struct {
		name            string
		meta            func() *waid.UserLoginMetadata
		requestFullSync bool
		maxWait         time.Duration
		now             time.Time
		want            time.Duration
	}{
		{"full sync unfinished", waiting, true, maxWait, loggedIn.Add(25 * time.Minute), maxWait - 25*time.Minute},
		{"full sync done", func() *waid.UserLoginMetadata {
			meta := waiting()
			meta.HistorySyncFullProgress = 100
			return meta
		}, true, maxWait, loggedIn.Add(time.Minute), 0},
		{"no full sync yet", func() *waid.UserLoginMetadata {
			meta := waiting()
			meta.HistorySyncFullProgress = 0
			return meta
		}, true, maxWait, loggedIn.Add(time.Minute), maxWait - time.Minute},
		{"wait ran out", waiting, true, maxWait, loggedIn.Add(maxWait + time.Second), 0},
		{"waiting disabled", waiting, true, 0, loggedIn.Add(time.Minute), 0},
		{"full sync not requested", waiting, false, maxWait, loggedIn.Add(time.Minute), 0},
		{"portals already created", func() *waid.UserLoginMetadata {
			meta := waiting()
			meta.HistorySyncPortalsNeedCreating = false
			return meta
		}, true, maxWait, loggedIn.Add(time.Minute), 0},
		{"no login time", func() *waid.UserLoginMetadata {
			meta := waiting()
			meta.LoggedInAt = jsontime.Unix{}
			return meta
		}, true, maxWait, loggedIn.Add(time.Minute), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := fullSyncWait(tc.meta(), tc.requestFullSync, tc.maxWait, tc.now); got != tc.want {
				t.Errorf("fullSyncWait() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestFillBridgeStateHistory(t *testing.T) {
	meta := &waid.UserLoginMetadata{
		LoggedInAt:                     jsontime.UnixNow(),
		PhoneLastSeen:                  jsontime.UnixNow(),
		HistorySyncPortalsNeedCreating: true,
		HistorySyncFullProgress:        65,
	}
	wa := &WhatsAppClient{UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{Metadata: meta}}}

	state := wa.FillBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	if state.StateEvent != status.StateConnected || state.Info["history"] != "started" || state.Info["history_progress"] != uint32(65) {
		t.Errorf("while history is under way, got %s %v", state.StateEvent, state.Info)
	}

	meta.HistorySyncPortalsNeedCreating = false
	state = wa.FillBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	if state.Info["history"] != "done" {
		t.Errorf("once portals are created, got %v", state.Info)
	}
	if _, ok := state.Info["history_progress"]; ok {
		t.Errorf("progress is only reported while history is under way, got %v", state.Info)
	}
}

func TestWaitGroupOrTimeout(t *testing.T) {
	var done sync.WaitGroup
	done.Add(1)
	go done.Done()
	if !waitGroupOrTimeout(context.Background(), &done, time.Second) {
		t.Error("a finished group should report finished")
	}

	var stuck sync.WaitGroup
	stuck.Add(1)
	if waitGroupOrTimeout(context.Background(), &stuck, 10*time.Millisecond) {
		t.Error("a group that never finishes should time out")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitGroupOrTimeout(ctx, &stuck, time.Hour) {
		t.Error("an ended context should stop the wait")
	}
}
