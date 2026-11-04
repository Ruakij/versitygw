// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package posix

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/versity/versitygw/backend/meta"
)

func TestLifecycleScheduleNext(t *testing.T) {
	utc := func(y int, m time.Month, d, h, min, s int) time.Time {
		return time.Date(y, m, d, h, min, s, 0, time.UTC)
	}
	tests := []struct {
		name     string
		interval time.Duration
		start    time.Duration
		now      time.Time
		want     time.Time
	}{
		{"default before midnight", 24 * time.Hour, 0, utc(2030, 1, 1, 10, 30, 0), utc(2030, 1, 2, 0, 0, 0)},
		{"default just after midnight", 24 * time.Hour, 0, utc(2030, 1, 1, 0, 0, 1), utc(2030, 1, 2, 0, 0, 0)},
		{"exactly on the boundary", 24 * time.Hour, 0, utc(2030, 1, 2, 0, 0, 0), utc(2030, 1, 3, 0, 0, 0)},
		{"just before the boundary", 24 * time.Hour, 0, utc(2030, 1, 1, 23, 59, 59), utc(2030, 1, 2, 0, 0, 0)},
		{"custom start later today", 24 * time.Hour, 3*time.Hour + 15*time.Minute, utc(2030, 1, 1, 2, 0, 0), utc(2030, 1, 1, 3, 15, 0)},
		{"custom start passed", 24 * time.Hour, 3*time.Hour + 15*time.Minute, utc(2030, 1, 1, 4, 0, 0), utc(2030, 1, 2, 3, 15, 0)},
		{"custom start with seconds", 24 * time.Hour, 23*time.Hour + 59*time.Minute + 59*time.Second, utc(2030, 1, 1, 12, 0, 0), utc(2030, 1, 1, 23, 59, 59)},
		{"start later than now at midnight", 24 * time.Hour, 6 * time.Hour, utc(2030, 1, 1, 0, 0, 0), utc(2030, 1, 1, 6, 0, 0)},
		{"6h", 6 * time.Hour, 0, utc(2030, 1, 1, 10, 30, 0), utc(2030, 1, 1, 12, 0, 0)},
		{"6h on the boundary", 6 * time.Hour, 0, utc(2030, 1, 1, 12, 0, 0), utc(2030, 1, 1, 18, 0, 0)},
		{"6h into the next day", 6 * time.Hour, 0, utc(2030, 1, 1, 18, 0, 1), utc(2030, 1, 2, 0, 0, 0)},
		{"6h with start 01:30", 6 * time.Hour, 90 * time.Minute, utc(2030, 1, 1, 10, 30, 0), utc(2030, 1, 1, 13, 30, 0)},
		{"6h with start 01:30 before it", 6 * time.Hour, 90 * time.Minute, utc(2030, 1, 1, 0, 30, 0), utc(2030, 1, 1, 1, 30, 0)},
		{"30m", 30 * time.Minute, 0, utc(2030, 1, 1, 10, 30, 0), utc(2030, 1, 1, 11, 0, 0)},
		// the 7h grid has a slot at 06:00, 13:00 and 20:00 on 2030-01-01, so
		// the next day starts three hours later in the grid
		{"7h does not divide 24h, day one", 7 * time.Hour, 0, utc(2030, 1, 1, 0, 0, 0), utc(2030, 1, 1, 6, 0, 0)},
		{"7h does not divide 24h, day two", 7 * time.Hour, 0, utc(2030, 1, 2, 0, 0, 0), utc(2030, 1, 2, 3, 0, 0)},
		{"before the epoch", 24 * time.Hour, 0, time.Unix(-3600, 0), time.Unix(0, 0).UTC()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := lifecycleSchedule{interval: tt.interval, start: tt.start}
			assert.Equal(t, tt.want, s.next(tt.now))
		})
	}
}

func TestParseLifecycleStart(t *testing.T) {
	ok := map[string]time.Duration{
		"00:00":    0,
		"06:30":    6*time.Hour + 30*time.Minute,
		"6:30":     6*time.Hour + 30*time.Minute,
		"23:59":    23*time.Hour + 59*time.Minute,
		"23:59:59": 23*time.Hour + 59*time.Minute + 59*time.Second,
		"01:02:03": time.Hour + 2*time.Minute + 3*time.Second,
	}
	for in, want := range ok {
		got, err := ParseLifecycleStart(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "24:00", "12:60", "12:00:60", "-1:00", "noon", "12", "12:00:00:00", "12:00 UTC", "1200"} {
		_, err := ParseLifecycleStart(in)
		assert.Error(t, err, in)
	}
}

// lifecycleRecorder replaces the clock, the timer and the work of the bucket
// tasks of a gateway.
type lifecycleRecorder struct {
	mu    sync.Mutex
	clock time.Time
	waits []time.Time
	runs  []string
}

func (r *lifecycleRecorder) now() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.clock
}

func (r *lifecycleRecorder) record(bucket string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, fmt.Sprintf("%s %s", r.clock.Format("2006-01-02 15:04:05"), bucket))
}

func newScheduleGateway(t *testing.T, buckets ...string) *Posix {
	t.Helper()
	p, err := New(t.TempDir(), meta.XattrMeta{}, PosixOpts{AbsolutePaths: true})
	require.NoError(t, err)
	t.Cleanup(p.Shutdown)
	for _, b := range buckets {
		createTestBucket(t, p, b)
		cfg := "<LifecycleConfiguration>" + lcRule("Enabled", lcPrefix(""), lcExpireDays(1)) + "</LifecycleConfiguration>"
		require.NoError(t, p.PutBucketLifecycleConfiguration(context.Background(), b, []byte(cfg)))
	}
	return p
}

func TestLifecycleScheduleRuns(t *testing.T) {
	p := newScheduleGateway(t, "a")
	r := &lifecycleRecorder{clock: lcBase}
	reached := make(chan struct{})
	p.now = r.now
	p.lifecycleBucketFn = func(ctx context.Context, bucket string) { r.record(bucket) }
	p.lifecycleWaitFn = func(ctx context.Context, at time.Time) bool {
		// a run of the previous slot has ended before the clock moves on
		p.lifecycleTasks.Wait()
		r.mu.Lock()
		r.waits = append(r.waits, at)
		n := len(r.waits)
		if n <= 5 {
			r.clock = at
		}
		r.mu.Unlock()
		if n > 5 {
			close(reached)
			<-ctx.Done()
			return false
		}
		return true
	}

	p.startLifecycle(lifecycleSchedule{interval: 6 * time.Hour}, 90*time.Second)
	<-reached
	p.stopLifecycle()

	var waits []string
	for _, w := range r.waits {
		waits = append(waits, w.Format("2006-01-02 15:04:05"))
	}
	// the catch-up run, then the slots; the slot at 12:00 is not run twice
	assert.Equal(t, []string{
		"2030-01-01 10:31:30",
		"2030-01-01 12:00:00",
		"2030-01-01 18:00:00",
		"2030-01-02 00:00:00",
		"2030-01-02 06:00:00",
		"2030-01-02 12:00:00",
	}, waits)
	assert.Equal(t, []string{
		"2030-01-01 10:31:30 a",
		"2030-01-01 12:00:00 a",
		"2030-01-01 18:00:00 a",
		"2030-01-02 00:00:00 a",
		"2030-01-02 06:00:00 a",
	}, r.runs)
}

func TestLifecycleCatchUpOnly(t *testing.T) {
	// A catch-up run past the next slot does not run that slot as well.
	p := newScheduleGateway(t, "a")
	r := &lifecycleRecorder{clock: time.Date(2030, 1, 1, 23, 59, 30, 0, time.UTC)}
	reached := make(chan struct{})
	p.now = r.now
	p.lifecycleBucketFn = func(ctx context.Context, bucket string) { r.record(bucket) }
	p.lifecycleWaitFn = func(ctx context.Context, at time.Time) bool {
		p.lifecycleTasks.Wait()
		r.mu.Lock()
		r.waits = append(r.waits, at)
		n := len(r.waits)
		if n <= 2 {
			r.clock = at
		}
		r.mu.Unlock()
		if n > 2 {
			close(reached)
			<-ctx.Done()
			return false
		}
		return true
	}

	p.startLifecycle(lifecycleSchedule{interval: 24 * time.Hour}, 90*time.Second)
	<-reached
	p.stopLifecycle()

	assert.Equal(t, []string{"2030-01-02 00:01:00 a", "2030-01-03 00:00:00 a"}, r.runs)
}

// blockingBuckets counts the bucket tasks that run at the same time.
type blockingBuckets struct {
	mu      sync.Mutex
	running int
	max     int
	started map[string]int
	release chan struct{}
}

func newBlockingBuckets() *blockingBuckets {
	return &blockingBuckets{started: map[string]int{}, release: make(chan struct{})}
}

func (b *blockingBuckets) run(ctx context.Context, bucket string) {
	b.mu.Lock()
	b.running++
	b.max = max(b.max, b.running)
	b.started[bucket]++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.running--
		b.mu.Unlock()
	}()
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}

func (b *blockingBuckets) snapshot() (running int, started map[string]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	started = map[string]int{}
	for k, v := range b.started {
		started[k] = v
	}
	return b.running, started
}

func TestLifecycleConcurrencyBound(t *testing.T) {
	names := []string{"b1", "b2", "b3", "b4", "b5", "b6"}
	p := newScheduleGateway(t, names...)
	p.lifecycleConcurrency = 2
	work := newBlockingBuckets()
	p.lifecycleBucketFn = work.run

	finished := make(chan struct{})
	go func() {
		p.runLifecycle(context.Background())
		close(finished)
	}()

	require.Eventually(t, func() bool { r, _ := work.snapshot(); return r == 2 }, 5*time.Second, time.Millisecond)
	// the other tasks wait for a slot
	time.Sleep(50 * time.Millisecond)
	r, started := work.snapshot()
	assert.Equal(t, 2, r)
	assert.Len(t, started, 2)

	// each release lets one waiting task in
	for range names {
		work.release <- struct{}{}
	}
	<-finished

	_, started = work.snapshot()
	var got []string
	for k, n := range started {
		assert.Equal(t, 1, n, k)
		got = append(got, k)
	}
	sort.Strings(got)
	assert.Equal(t, names, got)
	assert.Equal(t, 2, work.max)
}

func TestLifecycleConcurrencyDefault(t *testing.T) {
	p := newScheduleGateway(t)
	assert.Equal(t, DefaultLifecycleConcurrency, cap(p.lifecycleSlots()))
}

func TestLifecycleStillRunningBucketSkipped(t *testing.T) {
	p := newScheduleGateway(t, "slow", "fast")
	p.lifecycleConcurrency = 4
	slow := make(chan struct{})
	var mu sync.Mutex
	started := map[string]int{}
	p.lifecycleBucketFn = func(ctx context.Context, bucket string) {
		mu.Lock()
		started[bucket]++
		mu.Unlock()
		if bucket == "slow" {
			select {
			case <-slow:
			case <-ctx.Done():
			}
		}
	}
	count := func(b string) int {
		mu.Lock()
		defer mu.Unlock()
		return started[b]
	}

	ctx := context.Background()
	p.dispatchLifecycle(ctx)
	require.Eventually(t, func() bool { return count("slow") == 1 && count("fast") == 1 }, 5*time.Second, time.Millisecond)

	// the next slot: the slow bucket is still running, the fast one starts
	p.dispatchLifecycle(ctx)
	require.Eventually(t, func() bool { return count("fast") == 2 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 1, count("slow"))

	// once it ends, the bucket takes part again
	close(slow)
	require.Eventually(t, func() bool {
		p.lifecycleMu.Lock()
		defer p.lifecycleMu.Unlock()
		return !p.lifecycleRunning["slow"]
	}, 5*time.Second, time.Millisecond)
	p.dispatchLifecycle(ctx)
	p.lifecycleTasks.Wait()
	assert.Equal(t, 2, count("slow"))
	assert.Equal(t, 3, count("fast"))
}

func TestLifecycleShutdownWithTasksInFlight(t *testing.T) {
	p := newScheduleGateway(t, "b1", "b2", "b3")
	p.lifecycleConcurrency = 2
	work := newBlockingBuckets()
	p.lifecycleBucketFn = work.run
	first := true
	p.lifecycleWaitFn = func(ctx context.Context, at time.Time) bool {
		if first {
			first = false
			return true
		}
		<-ctx.Done()
		return false
	}

	p.startLifecycle(lifecycleSchedule{interval: time.Hour}, 0)
	// two tasks run, the third waits for a slot
	require.Eventually(t, func() bool { r, _ := work.snapshot(); return r == 2 }, 5*time.Second, time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		p.Shutdown()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not end the bucket tasks")
	}

	running, started := work.snapshot()
	assert.Zero(t, running)
	assert.Len(t, started, 2, "the task waiting for a slot never started")
}

func TestLifecycleBucketErrorStaysInBucket(t *testing.T) {
	e := newLifecycleEnv(t, metaModes(t)["xattr"], false, false)
	e.setRules(lcRule("Enabled", lcPrefix(""), lcExpireDays(1)))
	e.put("a", "x", lcBase)
	e.clock = lcDay(5)

	createTestBucket(t, e.p, "broken")
	require.NoError(t, e.p.meta.StoreAttribute(nil, "broken", "", lifecyclekey, []byte("<not xml")))

	e.p.runLifecycle(context.Background())
	assert.Empty(t, e.keys())
}
