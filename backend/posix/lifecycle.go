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
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

const (
	// lifecycleFirstRunDelay is the time between the start of the gateway
	// and the first lifecycle run, before the jitter is added.
	lifecycleFirstRunDelay = time.Minute
	// defaultLifecyclePageSize is the number of versions requested per
	// listing page.
	defaultLifecyclePageSize = 1000
	// DefaultLifecycleConcurrency is the number of buckets whose lifecycle
	// rules are applied at the same time.
	DefaultLifecycleConcurrency = 4
)

// lifecycleStats counts what a lifecycle run did in one bucket.
type lifecycleStats struct {
	deleted        int
	markersCreated int
	markersRemoved int
	aborted        int
	skippedLocked  int
	raced          int
	errors         int
}

// lifecycleRule is an enabled rule with its object selection resolved.
type lifecycleRule struct {
	*s3response.LifecycleRule
	criteria s3response.LifecycleCriteria
}

// lifecycleBucket is the state of one bucket during a run.
type lifecycleBucket struct {
	name   string
	status types.BucketVersioningStatus
	now    time.Time
	rules  []lifecycleRule
}

// lifecycleVersion is one version or delete marker of a key.
type lifecycleVersion struct {
	id      string
	modTime time.Time
	size    int64
	marker  bool
	// current is the version at the object path, as opposed to the ones
	// in the versioning directory.
	current bool
	tags    map[string]string
}

// lifecycleKey holds the versions of a key, newest first once merged.
type lifecycleKey struct {
	key      string
	versions []s3response.ObjectVersion
	markers  []s3response.DeleteMarkerEntry
}

// ParseLifecycleStart parses the time of day, in UTC, that anchors the
// lifecycle schedule: HH:MM or HH:MM:SS from 00:00 to 23:59:59. It returns
// the offset from midnight.
func ParseLifecycleStart(s string) (time.Duration, error) {
	for _, layout := range []string{"15:04", "15:04:05"} {
		t, err := time.Parse(layout, s)
		if err != nil {
			continue
		}
		return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second, nil
	}
	return 0, fmt.Errorf("invalid lifecycle start %q: want a time of day in UTC as HH:MM or HH:MM:SS, from 00:00 to 23:59:59", s)
}

// lifecycleSchedule places the runs of the lifecycle rules on a grid of
// the interval anchored at the Unix epoch plus start. The grid does not
// depend on when the gateway started, so gateways and restarts agree on the
// slots. An interval that does not divide 24h drifts against the time of
// day from one day to the next.
type lifecycleSchedule struct {
	interval time.Duration
	start    time.Duration
}

// next returns the first slot after now. A slot at now itself has been
// taken already.
func (s lifecycleSchedule) next(now time.Time) time.Time {
	iv := s.interval.Nanoseconds()
	rel := now.UnixNano() - s.start.Nanoseconds()
	k := rel / iv
	if rel%iv < 0 {
		k--
	}
	return time.Unix(0, (k+1)*iv+s.start.Nanoseconds()).UTC()
}

// startLifecycle starts the goroutine that applies the lifecycle rules of
// all buckets once after firstDelay, to catch up after a start, and then at
// every slot of the schedule.
func (p *Posix) startLifecycle(sched lifecycleSchedule, firstDelay time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	p.lifecycleCancel = cancel
	p.lifecycleDone = done

	go func() {
		defer close(done)
		if !p.lifecycleWait(ctx, p.now().Add(firstDelay)) {
			return
		}
		p.dispatchLifecycle(ctx)
		for {
			if !p.lifecycleWait(ctx, sched.next(p.now())) {
				return
			}
			p.dispatchLifecycle(ctx)
		}
	}()
}

// lifecycleWait blocks until the clock reaches t and reports false when ctx
// ends first.
func (p *Posix) lifecycleWait(ctx context.Context, t time.Time) bool {
	if p.lifecycleWaitFn != nil {
		return p.lifecycleWaitFn(ctx, t)
	}
	timer := time.NewTimer(t.Sub(p.now()))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// stopLifecycle stops the lifecycle goroutine and cancels the bucket tasks
// it started, then waits for all of them to end.
func (p *Posix) stopLifecycle() {
	if p.lifecycleCancel == nil {
		return
	}
	p.lifecycleCancel()
	<-p.lifecycleDone
	p.lifecycleTasks.Wait()
}

// lifecycleStartDelay returns the delay of the catch-up run: spreading the
// catch-up runs of gateways started together keeps them from walking a shared
// filesystem at the same time.
func lifecycleStartDelay() time.Duration {
	return lifecycleFirstRunDelay + time.Duration(rand.Int64N(int64(lifecycleFirstRunDelay)))
}

// dispatchLifecycle starts a task for every bucket with a lifecycle
// configuration and returns without waiting for them. A bucket whose
// previous task has not ended gets no new one; the other buckets are not
// affected by it. At most p.lifecycleConcurrency tasks run at once, the rest
// wait for a slot.
func (p *Posix) dispatchLifecycle(ctx context.Context) {
	fis, err := listBucketFileInfos(p.rootPath(), p.bucketlinks)
	if err != nil {
		log.Printf("lifecycle: list buckets: %v", err)
		return
	}
	for _, fi := range fis {
		if ctx.Err() != nil {
			return
		}
		bucket := fi.Name()
		if p.isExcludedName(bucket) || !p.isBucketValid(bucket) {
			continue
		}
		if !p.hasLifecycleConfig(bucket) {
			continue
		}
		if !p.lifecycleClaim(bucket) {
			log.Printf("lifecycle: bucket %q: skipped, the previous run is still in progress", bucket)
			continue
		}
		p.lifecycleTasks.Add(1)
		go func() {
			defer p.lifecycleTasks.Done()
			defer p.lifecycleRelease(bucket)
			p.lifecycleTask(ctx, bucket)
		}()
	}
}

// hasLifecycleConfig reports whether the bucket has a lifecycle
// configuration stored.
func (p *Posix) hasLifecycleConfig(bucket string) bool {
	_, err := p.meta.RetrieveAttribute(nil, bucket, "", lifecyclekey)
	return err == nil
}

// lifecycleClaim marks the bucket as running and reports false when it
// already is.
func (p *Posix) lifecycleClaim(bucket string) bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.lifecycleRunning[bucket] {
		return false
	}
	if p.lifecycleRunning == nil {
		p.lifecycleRunning = make(map[string]bool)
	}
	p.lifecycleRunning[bucket] = true
	return true
}

func (p *Posix) lifecycleRelease(bucket string) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	delete(p.lifecycleRunning, bucket)
}

// lifecycleSlots returns the channel that bounds the running bucket tasks.
func (p *Posix) lifecycleSlots() chan struct{} {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.lifecycleSem == nil {
		n := p.lifecycleConcurrency
		if n < 1 {
			n = DefaultLifecycleConcurrency
		}
		p.lifecycleSem = make(chan struct{}, n)
	}
	return p.lifecycleSem
}

// lifecycleTask waits for a free slot and applies the lifecycle rules of the
// bucket. A failure is logged and stays within the bucket.
func (p *Posix) lifecycleTask(ctx context.Context, bucket string) {
	slots := p.lifecycleSlots()
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return
	}

	if p.lifecycleBucketFn != nil {
		p.lifecycleBucketFn(ctx, bucket)
		return
	}
	stats, ok := p.lifecycleRunBucket(ctx, bucket)
	if !ok {
		return
	}
	log.Printf("lifecycle: bucket %q: deleted versions %d, delete markers created %d, "+
		"delete markers removed %d, uploads aborted %d, skipped locked %d, skipped changed %d, errors %d",
		bucket, stats.deleted, stats.markersCreated, stats.markersRemoved,
		stats.aborted, stats.skippedLocked, stats.raced, stats.errors)
}

// runLifecycle applies the lifecycle rules of every bucket once and waits
// for the tasks to end.
func (p *Posix) runLifecycle(ctx context.Context) {
	p.dispatchLifecycle(ctx)
	p.lifecycleTasks.Wait()
}

// loadLifecycleRules returns the enabled rules of the bucket with an action
// the gateway applies, or nil when the bucket has none.
func (p *Posix) loadLifecycleRules(bucket string) ([]lifecycleRule, error) {
	data, err := p.meta.RetrieveAttribute(nil, bucket, "", lifecyclekey)
	if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get lifecycle configuration: %w", err)
	}
	cfg, err := s3response.ParseLifecycleConfigOutput(data)
	if err != nil {
		return nil, fmt.Errorf("parse lifecycle configuration: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid lifecycle configuration: %w", err)
	}

	var rules []lifecycleRule
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		if !r.Enabled() {
			continue
		}
		if r.Expiration == nil && r.NoncurrentVersionExpiration == nil && r.AbortIncompleteMultipartUpload == nil {
			continue
		}
		rules = append(rules, lifecycleRule{LifecycleRule: r, criteria: r.Criteria()})
	}
	return rules, nil
}

// lifecycleRunBucket applies the rules of one bucket. It reports false when
// the bucket has nothing to do.
func (p *Posix) lifecycleRunBucket(ctx context.Context, bucket string) (lifecycleStats, bool) {
	var stats lifecycleStats

	rules, err := p.loadLifecycleRules(bucket)
	if err != nil {
		log.Printf("lifecycle: bucket %q: %v", bucket, err)
		return stats, false
	}
	if len(rules) == 0 {
		return stats, false
	}
	status, err := p.getBucketVersioningStatus(ctx, bucket)
	if err != nil {
		log.Printf("lifecycle: bucket %q: get versioning status: %v", bucket, err)
		return stats, false
	}

	b := &lifecycleBucket{name: bucket, status: status, now: p.now(), rules: rules}

	if slicesAnyRule(rules, func(r lifecycleRule) bool {
		return r.Expiration != nil || r.NoncurrentVersionExpiration != nil
	}) {
		if err := p.lifecycleWalk(ctx, b, &stats); err != nil && ctx.Err() == nil {
			log.Printf("lifecycle: bucket %q: walk: %v", bucket, err)
			stats.errors++
		}
	}
	if ctx.Err() == nil {
		p.lifecycleAbortUploads(ctx, b, &stats)
	}
	return stats, true
}

func slicesAnyRule(rules []lifecycleRule, f func(lifecycleRule) bool) bool {
	for _, r := range rules {
		if f(r) {
			return true
		}
	}
	return false
}

// lifecycleWalk lists the versions of the bucket page by page and applies
// the rules to each key once all its versions are known. The key a page ends
// in may continue on the next page, so it is kept back until then. The
// version the next page starts after belongs to that key, so no version
// the listing resumes from is deleted.
func (p *Posix) lifecycleWalk(ctx context.Context, b *lifecycleBucket, stats *lifecycleStats) error {
	var keyMarker, versionMarker string
	var pending *lifecycleKey

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := p.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:          &b.name,
			MaxKeys:         aws.Int32(int32(p.lifecyclePageSize)),
			KeyMarker:       &keyMarker,
			VersionIdMarker: &versionMarker,
		})
		if err != nil {
			return err
		}

		truncated := aws.ToBool(res.IsTruncated)
		nextKey := aws.ToString(res.NextKeyMarker)
		nextVersion := aws.ToString(res.NextVersionIdMarker)
		if truncated && nextKey == "" {
			return fmt.Errorf("listing truncated without a key marker")
		}

		groups := groupLifecycleKeys(res)
		if pending != nil {
			merged := false
			for _, g := range groups {
				if g.key == pending.key {
					g.versions = append(pending.versions, g.versions...)
					g.markers = append(pending.markers, g.markers...)
					merged = true
					break
				}
			}
			if !merged {
				p.lifecycleApplyKey(ctx, b, pending, stats)
			}
			pending = nil
		}
		for _, g := range groups {
			if truncated && g.key == nextKey {
				pending = g
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.lifecycleApplyKey(ctx, b, g, stats)
		}

		if !truncated {
			return nil
		}
		keyMarker, versionMarker = nextKey, nextVersion
	}
}

// groupLifecycleKeys splits a listing page into its keys, in listing order.
func groupLifecycleKeys(res s3response.ListVersionsResult) []*lifecycleKey {
	var groups []*lifecycleKey
	byKey := make(map[string]*lifecycleKey)
	get := func(key string) *lifecycleKey {
		g, ok := byKey[key]
		if !ok {
			g = &lifecycleKey{key: key}
			byKey[key] = g
			groups = append(groups, g)
		}
		return g
	}
	for _, v := range res.Versions {
		g := get(aws.ToString(v.Key))
		g.versions = append(g.versions, v)
	}
	for _, m := range res.DeleteMarkers {
		g := get(aws.ToString(m.Key))
		g.markers = append(g.markers, m)
	}
	return groups
}

// mergeLifecycleVersions returns the versions and delete markers of a key
// newest first. Both lists come newest first already; ids that are ULIDs
// order by creation, the null version has to be placed by its time.
func mergeLifecycleVersions(versions []s3response.ObjectVersion, markers []s3response.DeleteMarkerEntry) []*lifecycleVersion {
	vs := make([]*lifecycleVersion, 0, len(versions))
	for _, v := range versions {
		vs = append(vs, &lifecycleVersion{
			id:      aws.ToString(v.VersionId),
			modTime: aws.ToTime(v.LastModified),
			size:    aws.ToInt64(v.Size),
			current: aws.ToBool(v.IsLatest),
		})
	}
	ms := make([]*lifecycleVersion, 0, len(markers))
	for _, m := range markers {
		ms = append(ms, &lifecycleVersion{
			id:      aws.ToString(m.VersionId),
			modTime: aws.ToTime(m.LastModified),
			marker:  true,
			current: aws.ToBool(m.IsLatest),
		})
	}

	seen := make(map[string]struct{}, len(vs)+len(ms))
	out := make([]*lifecycleVersion, 0, len(vs)+len(ms))
	add := func(v *lifecycleVersion) {
		if _, dup := seen[v.id]; dup {
			return
		}
		seen[v.id] = struct{}{}
		out = append(out, v)
	}
	for len(vs) > 0 || len(ms) > 0 {
		switch {
		case len(ms) == 0:
			add(vs[0])
			vs = vs[1:]
		case len(vs) == 0:
			add(ms[0])
			ms = ms[1:]
		case lifecycleNewer(vs[0], ms[0]):
			add(vs[0])
			vs = vs[1:]
		default:
			add(ms[0])
			ms = ms[1:]
		}
	}
	return out
}

// lifecycleNewer reports whether a was created after b.
func lifecycleNewer(a, b *lifecycleVersion) bool {
	if a.current != b.current {
		return a.current
	}
	if a.id != nullVersionId && b.id != nullVersionId && a.id != b.id {
		return a.id > b.id
	}
	return a.modTime.After(b.modTime)
}

// lifecycleDue reports whether an object created at base is expired days
// later: S3 rounds the expiration time up to the next midnight UTC.
func lifecycleDue(base time.Time, days int32, now time.Time) bool {
	// calendar arithmetic: a time.Duration of days overflows
	t := base.UTC().AddDate(0, 0, int(days))
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	if day.Before(t) {
		day = day.AddDate(0, 0, 1)
	}
	return !now.Before(day)
}

// lifecycleApplyKey applies the rules to the versions of one key.
func (p *Posix) lifecycleApplyKey(ctx context.Context, b *lifecycleBucket, g *lifecycleKey, stats *lifecycleStats) {
	var rules []lifecycleRule
	for _, r := range b.rules {
		if (r.Expiration != nil || r.NoncurrentVersionExpiration != nil) && strings.HasPrefix(g.key, r.criteria.Prefix) {
			rules = append(rules, r)
		}
	}
	if len(rules) == 0 {
		return
	}

	vs := mergeLifecycleVersions(g.versions, g.markers)
	if len(vs) == 0 {
		return
	}
	// whatever sorts first is what the object path holds
	vs[0].current = true
	for _, v := range vs[1:] {
		v.current = false
	}

	// noncurrent versions first, so that a delete marker left alone
	// is found out in this run
	remaining := 0
	rank := make([]int, len(rules))
	for i := 1; i < len(vs); i++ {
		v := vs[i]
		since := p.lifecycleNoncurrentSince(b, g.key, vs[i-1])
		expire := false
		for ri, r := range rules {
			nve := r.NoncurrentVersionExpiration
			if nve == nil {
				continue
			}
			due := lifecycleDue(since, aws.ToInt32(nve.NoncurrentDays), b.now)
			if nve.NewerNoncurrentVersions == nil && !due {
				continue
			}
			ok, err := p.lifecycleMatch(ctx, b, g.key, r, v)
			if err != nil {
				p.lifecycleError(b, g.key, v, err, stats)
				continue
			}
			if !ok {
				continue
			}
			rank[ri]++
			if due && (nve.NewerNoncurrentVersions == nil || rank[ri] > int(*nve.NewerNoncurrentVersions)) {
				expire = true
			}
		}
		if !expire || !p.lifecycleDeleteVersion(ctx, b, g.key, v, stats) {
			remaining++
		}
	}

	cur := vs[0]
	if cur.marker {
		if remaining == 0 && p.lifecycleMarkerExpirable(b, g.key, cur) {
			p.lifecycleRemoveMarker(ctx, b, g.key, cur, stats)
		}
		return
	}
	for _, r := range rules {
		e := r.Expiration
		if e == nil || !lifecycleCurrentDue(e, cur, b.now) {
			continue
		}
		ok, err := p.lifecycleMatch(ctx, b, g.key, r, cur)
		if err != nil {
			p.lifecycleError(b, g.key, cur, err, stats)
			return
		}
		if ok {
			p.lifecycleExpireCurrent(ctx, b, g.key, cur, stats)
			return
		}
	}
}

// lifecycleCurrentDue reports whether the Days or Date of an expiration
// has passed for the current version. Like in S3, the Date applies to every
// matching version on and after it, also to the ones created later.
func lifecycleCurrentDue(e *s3response.LifecycleExpiration, v *lifecycleVersion, now time.Time) bool {
	switch {
	case e.Days != nil:
		return lifecycleDue(v.modTime, *e.Days, now)
	case e.Date != nil:
		return !now.Before(*e.Date)
	}
	return false
}

// lifecycleNoncurrentSince returns the time a version became noncurrent,
// the moment its successor was created. A delete marker takes the last
// modified time of the version it hides when the gateway cannot set its own,
// and every other change of the entry updates its change time, so the later
// of the two is the creation time of the marker or after it. Where the change
// time is not known (see fileChangeTime), a marker is aged by its last
// modified time alone. A sidecar store keeps attributes outside the entry,
// so only changes of the entry itself, such as moving a marker to the
// versioning directory, update its change time.
func (p *Posix) lifecycleNoncurrentSince(b *lifecycleBucket, key string, succ *lifecycleVersion) time.Time {
	since := succ.modTime
	if !succ.marker {
		return since
	}
	fi, err := os.Stat(p.lifecycleDataPath(b.name, key, succ))
	if err != nil {
		return since
	}
	if ct := fileChangeTime(fi); ct.After(since) {
		return ct
	}
	return since
}

// lifecycleMarkerExpirable reports whether a rule removes the delete marker
// of a key that has no other version: the ExpiredObjectDeleteMarker rule
// does, and so does an Expiration of Days or Date once it is due for the
// marker.
func (p *Posix) lifecycleMarkerExpirable(b *lifecycleBucket, key string, marker *lifecycleVersion) bool {
	for _, r := range b.rules {
		e := r.Expiration
		if e == nil || !strings.HasPrefix(key, r.criteria.Prefix) {
			continue
		}
		c := r.criteria
		if len(c.Tags) != 0 || c.MinSize != nil || c.MaxSize != nil {
			continue
		}
		if aws.ToBool(e.ExpiredObjectDeleteMarker) || lifecycleCurrentDue(e, marker, b.now) {
			return true
		}
	}
	return false
}

// lifecycleMatch reports whether the rule selects the version: the prefix
// is checked by the caller. A delete marker has no size and no tags, so it
// only matches a rule that has neither.
func (p *Posix) lifecycleMatch(ctx context.Context, b *lifecycleBucket, key string, r lifecycleRule, v *lifecycleVersion) (bool, error) {
	c := r.criteria
	if v.marker {
		return len(c.Tags) == 0 && c.MinSize == nil && c.MaxSize == nil, nil
	}
	if c.MinSize != nil && v.size <= *c.MinSize {
		return false, nil
	}
	if c.MaxSize != nil && v.size >= *c.MaxSize {
		return false, nil
	}
	if len(c.Tags) == 0 {
		return true, nil
	}
	if v.tags == nil {
		release, err := p.acquireActionSlot(ctx)
		if err != nil {
			return false, err
		}
		attrBucket, attrObject := p.lifecycleAttrPath(b.name, key, v)
		tags, err := p.getAttrTags(attrBucket, attrObject, v.id)
		release()
		if err != nil {
			return false, fmt.Errorf("get tags: %w", err)
		}
		v.tags = tags
	}
	for _, t := range c.Tags {
		if val, ok := v.tags[t.Key]; !ok || val != t.Value {
			return false, nil
		}
	}
	return true, nil
}

// lifecycleAttrPath returns the bucket and object the metadata of the
// version is stored under.
func (p *Posix) lifecycleAttrPath(bucket, key string, v *lifecycleVersion) (string, string) {
	enc := p.encodeKey(key)
	if v.current {
		return bucket, enc
	}
	return filepath.Join(p.versioningDir, bucket), filepath.Join(genObjVersionKey(enc), v.id)
}

// lifecycleDataPath returns the path of the file that stores the version.
func (p *Posix) lifecycleDataPath(bucket, key string, v *lifecycleVersion) string {
	attrBucket, attrObject := p.lifecycleAttrPath(bucket, key, v)
	if v.current {
		return p.ObjectPath(attrBucket, attrObject)
	}
	return filepath.Join(attrBucket, attrObject)
}

// lifecycleVersionLocked reports whether a legal hold or a retention that
// has not passed protects the version, whatever the retention mode and
// whatever the bucket's Object Lock configuration says. It fails closed: a
// version whose lock cannot be read counts as locked.
func (p *Posix) lifecycleVersionLocked(ctx context.Context, b *lifecycleBucket, key string, v *lifecycleVersion) (bool, error) {
	release, err := p.acquireActionSlot(ctx)
	if err != nil {
		return true, err
	}
	defer release()

	attrBucket, attrObject := p.lifecycleAttrPath(b.name, key, v)
	return p.lifecycleLockedAt(p.lifecycleDataPath(b.name, key, v), attrBucket, attrObject, b.now)
}

// lifecycleLockedAt is lifecycleVersionLocked for the version whose file is
// dataPath and whose attributes are stored at attrBucket and attrObject.
func (p *Posix) lifecycleLockedAt(dataPath, attrBucket, attrObject string, now time.Time) (bool, error) {
	// A file-based metadata store reports a version that does not exist
	// like one without the attribute, so a missing lock only counts for a
	// version known to exist.
	if _, err := os.Stat(dataPath); err != nil {
		return true, fmt.Errorf("stat version: %w", err)
	}

	hold, err := p.meta.RetrieveAttribute(nil, attrBucket, attrObject, objectLegalHoldKey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return true, fmt.Errorf("get legal hold: %w", err)
	}
	if err == nil && len(hold) > 0 && hold[0] == 1 {
		return true, nil
	}

	data, err := p.meta.RetrieveAttribute(nil, attrBucket, attrObject, objectRetentionKey)
	if errors.Is(err, meta.ErrNoSuchKey) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("get retention: %w", err)
	}
	retention, err := auth.ParseObjectLockRetentionOutput(data)
	if err != nil {
		return true, fmt.Errorf("parse retention: %w", err)
	}
	if retention.RetainUntilDate != nil && retention.RetainUntilDate.After(now) {
		return true, nil
	}
	if retention.RetainUntilDate == nil && retention.Mode != "" {
		return true, fmt.Errorf("retention without a date")
	}
	return false, nil
}

func (p *Posix) lifecycleError(b *lifecycleBucket, key string, v *lifecycleVersion, err error, stats *lifecycleStats) {
	if isLifecycleGone(err) {
		stats.raced++
		return
	}
	stats.errors++
	log.Printf("lifecycle: bucket %q: key %q version %q: %v", b.name, key, v.id, err)
}

// isLifecycleGone reports whether err says the entry was removed or
// replaced since it was listed, by a client or another gateway.
func isLifecycleGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchKey)) ||
		errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchVersion)) ||
		errors.Is(err, s3err.GetAPIError(s3err.ErrNoSuchUpload)) ||
		errors.Is(err, s3err.GetAPIError(s3err.ErrPreconditionFailed))
}

// lifecycleRemoval is the outcome of removing a version.
type lifecycleRemoval int

const (
	lifecycleRemoved lifecycleRemoval = iota
	// lifecycleGone means the version did not exist any more.
	lifecycleGone
	// lifecycleChanged means the key is not in the state it was listed in.
	lifecycleChanged
	lifecycleProtected
)

// lifecycleRemoveVersion permanently deletes a version, or a delete marker,
// unless it is protected. It never goes through DeleteObject: the state it
// decided on can change between the listing and the delete, and a version
// that became the current one by then, possibly with a retention, must stay.
// So everything is checked again under the publish lock of the key, which
// is held by the operations that replace the current version. An error is
// returned for entries that could not be examined, which are left alone.
func (p *Posix) lifecycleRemoveVersion(ctx context.Context, b *lifecycleBucket, key string, v *lifecycleVersion) (lifecycleRemoval, error) {
	release, err := p.acquireActionSlot(ctx)
	if err != nil {
		return lifecycleChanged, err
	}
	defer release()

	if v.id == "" {
		return lifecycleChanged, fmt.Errorf("version without an id")
	}
	if err := p.validateVersionId(v.id); err != nil {
		return lifecycleChanged, err
	}
	enc := p.encodeKey(key)
	if v.current && strings.HasSuffix(enc, "/") {
		return lifecycleChanged, nil
	}

	unlock, err := p.lockObjectPublish(ctx, b.name, enc)
	if err != nil {
		return lifecycleChanged, err
	}
	defer unlock()

	isLive := false
	_, err = p.statLiveObject(b.name, enc)
	switch {
	case err == nil:
		liveID, err := p.meta.RetrieveAttribute(nil, b.name, enc, versionIdKey)
		if errors.Is(err, meta.ErrNoSuchKey) {
			liveID = []byte(nullVersionId)
		} else if err != nil {
			return lifecycleChanged, fmt.Errorf("get obj versionId: %w", err)
		}
		isLive = string(liveID) == v.id
	case !errors.Is(err, fs.ErrNotExist):
		return lifecycleChanged, fmt.Errorf("stat object: %w", err)
	}

	if isLive != v.current {
		return lifecycleChanged, nil
	}
	if v.current {
		isMarker, err := p.isObjDeleteMarker(b.name, enc)
		if err != nil {
			return lifecycleChanged, err
		}
		if !isMarker {
			return lifecycleChanged, nil
		}
		// the current marker hides versions that appeared: removing it
		// would make one of them current
		ents, err := os.ReadDir(p.genObjVersionPath(b.name, enc))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return lifecycleChanged, fmt.Errorf("read version dir: %w", err)
		}
		if len(ents) > 0 {
			return lifecycleChanged, nil
		}
	}

	attrBucket, attrObject := p.lifecycleAttrPath(b.name, key, v)
	dataPath := p.lifecycleDataPath(b.name, key, v)

	// The id does not say the entry is the one that was listed: a null
	// version, which has a fixed id, is replaced by a new one when the
	// bucket is suspended.
	fi, err := os.Stat(dataPath)
	if errors.Is(err, fs.ErrNotExist) {
		return lifecycleGone, nil
	}
	if err != nil {
		return lifecycleChanged, fmt.Errorf("stat version: %w", err)
	}
	if !fi.ModTime().Equal(v.modTime) || (!v.marker && fi.Size() != v.size) {
		return lifecycleChanged, nil
	}

	locked, err := p.lifecycleLockedAt(dataPath, attrBucket, attrObject, b.now)
	if errors.Is(err, fs.ErrNotExist) {
		return lifecycleGone, nil
	}
	if locked {
		return lifecycleProtected, err
	}

	err = os.Remove(dataPath)
	if errors.Is(err, fs.ErrNotExist) {
		return lifecycleGone, nil
	}
	if err != nil {
		return lifecycleChanged, fmt.Errorf("remove version: %w", err)
	}
	// the file is gone: a leftover attribute is garbage, not a reason to
	// report the version as kept
	_ = p.meta.DeleteAttributes(attrBucket, attrObject)
	if v.current {
		p.removeParents(b.name, enc)
	} else {
		p.removeParents(filepath.Join(p.versioningDir, b.name), attrObject)
	}
	return lifecycleRemoved, nil
}

// lifecycleDeleteVersion permanently deletes a noncurrent version unless it
// is locked. It reports whether the version is gone.
func (p *Posix) lifecycleDeleteVersion(ctx context.Context, b *lifecycleBucket, key string, v *lifecycleVersion, stats *lifecycleStats) bool {
	res, err := p.lifecycleRemoveVersion(ctx, b, key, v)
	if err != nil {
		p.lifecycleError(b, key, v, err, stats)
		return false
	}
	switch res {
	case lifecycleRemoved:
		stats.deleted++
		return true
	case lifecycleGone:
		stats.raced++
		return true
	case lifecycleProtected:
		stats.skippedLocked++
	default:
		stats.raced++
	}
	return false
}

// lifecycleRemoveMarker removes a delete marker that hides no version.
func (p *Posix) lifecycleRemoveMarker(ctx context.Context, b *lifecycleBucket, key string, v *lifecycleVersion, stats *lifecycleStats) {
	res, err := p.lifecycleRemoveVersion(ctx, b, key, v)
	if err != nil {
		p.lifecycleError(b, key, v, err, stats)
		return
	}
	switch res {
	case lifecycleRemoved:
		stats.markersRemoved++
	case lifecycleProtected:
		stats.skippedLocked++
	default:
		stats.raced++
	}
}

// lifecycleExpireCurrent expires the current version. With versioning
// enabled that hides it behind a delete marker, which Object Lock does not
// prevent. Otherwise the version is deleted for good and has to be unlocked.
// The delete is conditional on the version listed, so a newer upload to the
// key is not deleted.
func (p *Posix) lifecycleExpireCurrent(ctx context.Context, b *lifecycleBucket, key string, v *lifecycleVersion, stats *lifecycleStats) {
	versioned := b.status == types.BucketVersioningStatusEnabled
	if !versioned {
		locked, err := p.lifecycleVersionLocked(ctx, b, key, v)
		if locked {
			if err != nil {
				p.lifecycleError(b, key, v, err, stats)
			} else {
				stats.skippedLocked++
			}
			return
		}
	}
	_, err := p.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:                  &b.name,
		Key:                     &key,
		IfMatchLastModifiedTime: aws.Time(v.modTime),
		IfMatchSize:             aws.Int64(v.size),
	})
	if err != nil {
		p.lifecycleError(b, key, v, err, stats)
		return
	}
	if versioned {
		stats.markersCreated++
	} else {
		stats.deleted++
	}
}

// lifecycleAbortUploads aborts the multipart uploads that exceed the
// DaysAfterInitiation of a matching rule. The initiation time is the
// modification time of the upload directory, as in ListMultipartUploads.
// Rules with a tag or size filter are rejected when stored: neither is known
// for an upload before it completes.
func (p *Posix) lifecycleAbortUploads(ctx context.Context, b *lifecycleBucket, stats *lifecycleStats) {
	var rules []lifecycleRule
	for _, r := range b.rules {
		c := r.criteria
		if r.AbortIncompleteMultipartUpload != nil && len(c.Tags) == 0 && c.MinSize == nil && c.MaxSize == nil {
			rules = append(rules, r)
		}
	}
	if len(rules) == 0 {
		return
	}

	root := p.ObjectPath(b.name, MetaTmpMultipartDir)
	keyDirs, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		log.Printf("lifecycle: bucket %q: read multipart directory: %v", b.name, err)
		stats.errors++
		return
	}

	for _, kd := range keyDirs {
		if ctx.Err() != nil {
			return
		}
		if !kd.IsDir() {
			continue
		}
		name, err := p.meta.RetrieveAttribute(nil, b.name, filepath.Join(MetaTmpMultipartDir, kd.Name()), onameAttr)
		if err != nil {
			continue
		}
		key := string(name)
		// the directory name is the hash of the key
		if fmt.Sprintf("%x", sha256.Sum256(name)) != kd.Name() {
			continue
		}

		var days int32 = -1
		for _, r := range rules {
			if !strings.HasPrefix(key, r.criteria.Prefix) {
				continue
			}
			d := aws.ToInt32(r.AbortIncompleteMultipartUpload.DaysAfterInitiation)
			if days < 0 || d < days {
				days = d
			}
		}
		if days < 0 {
			continue
		}

		uploads, err := os.ReadDir(filepath.Join(root, kd.Name()))
		if err != nil {
			continue
		}
		for _, u := range uploads {
			if !u.IsDir() || strings.HasSuffix(u.Name(), inProgressSuffix) {
				continue
			}
			fi, err := u.Info()
			if err != nil {
				continue
			}
			if !lifecycleDue(fi.ModTime(), days, b.now) {
				continue
			}
			id := u.Name()
			err = p.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:               &b.name,
				Key:                  &key,
				UploadId:             &id,
				IfMatchInitiatedTime: aws.Time(fi.ModTime()),
			})
			switch {
			case err == nil:
				stats.aborted++
			case isLifecycleGone(err):
				stats.raced++
			default:
				stats.errors++
				log.Printf("lifecycle: bucket %q: key %q upload %q: %v", b.name, key, id, err)
			}
		}
	}
}
