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

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

// Make series close
// - done when expired
// - see TestPurgeExpiredSeriesEmptySeries
// need to make the series written to disk
// - series will only close if no more "active" blocks. Block considered active
//   if series is in bucketMap in buffer - this is only removed when flushed to disk (I think?)

// Concurrently, purge it from wiredlist
// - done when removed from LRU cache
// - see TestWiredListInsertsAndUpdatesWiredBlocks

func TestWiredListPanic(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run.
	}

	tickInterval := 10 * time.Millisecond

	nsID := ident.StringID("ns0")
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(false).
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts).
		SetCacheBlocksOnRetrieve(true)
	ns, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)
	testOpts := NewTestOptions(t).
		// Smallest increment to make race condition more likely.
		SetTickMinimumInterval(tickInterval).
		SetTickCancellationCheckInterval(10 * time.Millisecond).
		SetNamespaces([]namespace.Metadata{ns}).
		SetMaxWiredBlocks(1)

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

	md := testSetup.NamespaceMetadataOrFail(nsID)
	ropts := md.Options().RetentionOptions()
	blockSize := ropts.BlockSize()
	filePathPrefix := testSetup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	start := testSetup.NowFn()()
	for i := 0; i < 1; i++ {
		write(t, testSetup, nsID, blockSize, start, filePathPrefix, i)
		time.Sleep(50 * time.Millisecond)
	}

	read(t, testSetup, nsID, blockSize, start)

	fmt.Printf("j>> %+v\n", "END SLEEP")
	time.Sleep(10 * time.Second)
}

func write(
	t *testing.T,
	testSetup TestSetup,
	nsID ident.ID,
	blockSize time.Duration,
	start time.Time,
	filePathPrefix string,
	i int,
) {
	blockStart := start.Add(time.Duration(2*i) * blockSize)
	testSetup.SetNowFn(blockStart)

	input := generate.BlockConfig{
		IDs: []string{"series0", "series1"}, NumPoints: 1, Start: blockStart,
	}
	testData := generate.Block(input)
	require.NoError(t, testSetup.WriteBatch(nsID, testData))

	testSetup.SetNowFn(blockStart.Add(blockSize * 3 / 2))
	require.NoError(t, waitUntilFileSetFilesExist(
		filePathPrefix,
		[]fs.FileSetFileIdentifier{
			{
				Namespace:   nsID,
				Shard:       1,
				BlockStart:  blockStart,
				VolumeIndex: 0,
			},
		},
		time.Minute,
	))
}

func read(
	t *testing.T,
	testSetup TestSetup,
	nsID ident.ID,
	blockSize time.Duration,
	start time.Time,
) {
	var (
		err error
		req = rpc.NewFetchRequest()
	)

	req.NameSpace = nsID.String()
	req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
	req.RangeEnd = xtime.ToNormalizedTime(start.Add(blockSize), time.Second)
	req.ResultTimeType = rpc.TimeType_UNIX_SECONDS

	for i := 0; i < 10; i++ {
		req.ID = "series0"
		_, err = testSetup.Fetch(req)
		require.NoError(t, err)

		req.ID = "series1"
		_, err = testSetup.Fetch(req)
		require.NoError(t, err)
	}
}
