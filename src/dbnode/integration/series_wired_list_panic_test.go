// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

// Make series close
// - done when expired
// - see TestPurgeExpiredSeriesEmptySeries

// Concurrently, purge it from wiredlist
// - done when removed from LRU cache
// - see TestWiredListInsertsAndUpdatesWiredBlocks

func TestWiredListPanic(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run.
	}

	nsID := ident.StringID("ns0")
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(false).
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts)
	ns, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)
	testOpts := NewTestOptions(t).
		// Smallest increment to make race condition more likely.
		SetTickMinimumInterval(10 * time.Millisecond).
		SetTickCancellationCheckInterval(10 * time.Millisecond).
		SetNamespaces([]namespace.Metadata{ns}).
		SetMaxWiredBlocks(1).
		SetNowFn(time.Now)

	testSetup, err := NewTestSetup(t, testOpts, nil,
		func(opt storage.Options) storage.Options {
			return opt.SetMediatorTickInterval(10 * time.Millisecond)
		},
	)

	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server.
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())
	log.Info("server is now up")

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Info("server is now down")
	}()

	cli := testSetup.M3DBClient()
	session, err := cli.DefaultSession()
	require.NoError(t, err)

	series0 := ident.StringID("series-0")
	session.Write(nsID, series0, time.Now(), 123, xtime.Second, nil)

	fmt.Printf("j>> %+v\n", "sleeping")
	time.Sleep(10 * time.Second)

	//HERE now it ticks quickly.
	// Make the tick close the series
	// Somehow concurrently evict from wiredlist
	// - Many alternating reads?
}
