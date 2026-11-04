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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3response"
)

// lcBase is the creation time of the test objects.
var lcBase = time.Date(2030, 1, 1, 10, 30, 0, 0, time.UTC)

func lcDay(n int) time.Time {
	return time.Date(2030, 1, n, 0, 0, 0, 0, time.UTC)
}

type lifecycleEnv struct {
	t      *testing.T
	p      *Posix
	bucket string
	clock  time.Time
}

type metaMaker = func(t *testing.T) (meta.MetadataStorer, PosixOpts)

// eachMeta runs the test with every metadata store.
func eachMeta(t *testing.T, test func(t *testing.T, mk metaMaker)) {
	t.Helper()
	for name, mk := range metaModes(t) {
		t.Run(name, func(t *testing.T) { test(t, mk) })
	}
}

// newLifecycleEnv creates a gateway with one bucket. With versioning the
// bucket has versioning enabled, with lock also Object Lock.
func newLifecycleEnv(t *testing.T, mk metaMaker, versioning, lock bool) *lifecycleEnv {
	t.Helper()
	storer, opts := mk(t)
	opts.AbsolutePaths = true
	if versioning {
		opts.VersioningDir = t.TempDir()
	}
	p, err := New(t.TempDir(), storer, opts)
	require.NoError(t, err)
	t.Cleanup(p.Shutdown)

	e := &lifecycleEnv{t: t, p: p, bucket: "bucket", clock: lcBase}
	p.now = func() time.Time { return e.clock }
	createTestBucket(t, p, e.bucket)
	if versioning {
		require.NoError(t, p.PutBucketVersioning(context.Background(), e.bucket, types.BucketVersioningStatusEnabled))
	}
	if lock {
		cfg, err := json.Marshal(auth.BucketLockConfig{Enabled: true})
		require.NoError(t, err)
		require.NoError(t, p.PutObjectLockConfiguration(context.Background(), e.bucket, cfg))
	}
	return e
}

func lcRule(status, filter, actions string) string {
	return fmt.Sprintf("<Rule><Status>%s</Status>%s%s</Rule>", status, filter, actions)
}

func lcPrefix(prefix string) string {
	return "<Filter><Prefix>" + prefix + "</Prefix></Filter>"
}

func lcExpireDays(days int) string {
	return fmt.Sprintf("<Expiration><Days>%d</Days></Expiration>", days)
}

func lcNoncurrent(days int, newer int) string {
	s := fmt.Sprintf("<NoncurrentDays>%d</NoncurrentDays>", days)
	if newer > 0 {
		s += fmt.Sprintf("<NewerNoncurrentVersions>%d</NewerNoncurrentVersions>", newer)
	}
	return "<NoncurrentVersionExpiration>" + s + "</NoncurrentVersionExpiration>"
}

const lcExpiredMarker = "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>"

func (e *lifecycleEnv) setRules(rules ...string) {
	e.t.Helper()
	cfg := "<LifecycleConfiguration>" + strings.Join(rules, "") + "</LifecycleConfiguration>"
	require.NoError(e.t, e.p.PutBucketLifecycleConfiguration(context.Background(), e.bucket, []byte(cfg)))
}

// put writes an object with the last modified time ts.
func (e *lifecycleEnv) put(key, body string, ts time.Time) string {
	e.t.Helper()
	out, err := e.p.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        &e.bucket,
		Key:           &key,
		Body:          strings.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	require.NoError(e.t, err)
	e.setTime(key, out.VersionID, ts)
	return out.VersionID
}

// del creates a delete marker with the last modified time ts.
func (e *lifecycleEnv) del(key string, ts time.Time) string {
	e.t.Helper()
	out, err := e.p.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &e.bucket, Key: &key})
	require.NoError(e.t, err)
	id := aws.ToString(out.VersionId)
	e.setTime(key, id, ts)
	return id
}

func (e *lifecycleEnv) setTime(key, id string, ts time.Time) {
	e.t.Helper()
	path := e.p.ObjectPath(e.bucket, key)
	if id != "" && e.p.versioningDir != "" {
		vp := filepath.Join(e.p.genObjVersionPath(e.bucket, key), id)
		if _, err := os.Stat(vp); err == nil {
			path = vp
		}
	}
	require.NoError(e.t, os.Chtimes(path, ts, ts))
}

func (e *lifecycleEnv) run() {
	e.t.Helper()
	e.p.runLifecycle(context.Background())
}

// runAt runs the lifecycle rules with the clock at ts.
func (e *lifecycleEnv) runAt(ts time.Time) {
	e.t.Helper()
	e.clock = ts
	e.run()
}

// versions lists the versions as "key/id", delete markers as "key/id (marker)".
func (e *lifecycleEnv) versions() []string {
	e.t.Helper()
	res, err := e.p.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket:  &e.bucket,
		MaxKeys: aws.Int32(1000),
	})
	require.NoError(e.t, err)
	var out []string
	for _, v := range res.Versions {
		out = append(out, aws.ToString(v.Key)+"/"+aws.ToString(v.VersionId))
	}
	for _, m := range res.DeleteMarkers {
		out = append(out, aws.ToString(m.Key)+"/"+aws.ToString(m.VersionId)+" (marker)")
	}
	sort.Strings(out)
	return out
}

func (e *lifecycleEnv) keys() []string {
	e.t.Helper()
	seen := map[string]bool{}
	var out []string
	for _, v := range e.versions() {
		k := v[:strings.LastIndex(v, "/")]
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func (e *lifecycleEnv) exists(key, id string) bool {
	e.t.Helper()
	for _, v := range e.versions() {
		if v == key+"/"+id {
			return true
		}
	}
	return false
}

func (e *lifecycleEnv) markers() int {
	e.t.Helper()
	n := 0
	for _, v := range e.versions() {
		if strings.HasSuffix(v, " (marker)") {
			n++
		}
	}
	return n
}

func TestLifecycleDue(t *testing.T) {
	created := time.Date(2030, 1, 1, 10, 30, 0, 0, time.UTC)
	tests := []struct {
		name string
		days int32
		now  time.Time
		due  bool
	}{
		{"before the midnight after the age", 1, time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC).Add(-time.Nanosecond), false},
		{"at the midnight after the age", 1, time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC), true},
		{"after the midnight after the age", 1, time.Date(2030, 1, 3, 5, 0, 0, 0, time.UTC), true},
		{"age reached but not the midnight", 1, time.Date(2030, 1, 2, 12, 0, 0, 0, time.UTC), false},
		{"seven days", 7, time.Date(2030, 1, 9, 0, 0, 0, 0, time.UTC), true},
		{"seven days early", 7, time.Date(2030, 1, 8, 23, 59, 59, 0, time.UTC), false},
		{"days that overflow a duration of nanoseconds", 106752, time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"years of days", 200000, time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"largest days", 2147483647, time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC), false},
		{"largest days far in the future", 2147483647, time.Date(9000000, 1, 1, 0, 0, 0, 0, time.UTC), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.due, lifecycleDue(created, tt.days, tt.now))
		})
	}

	t.Run("created at midnight", func(t *testing.T) {
		mid := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		assert.False(t, lifecycleDue(mid, 1, time.Date(2030, 1, 1, 23, 0, 0, 0, time.UTC)))
		assert.True(t, lifecycleDue(mid, 1, time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC)))
	})
	t.Run("zone is ignored", func(t *testing.T) {
		zone := time.FixedZone("x", 5*3600)
		assert.Equal(t,
			lifecycleDue(created, 1, time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC)),
			lifecycleDue(created.In(zone), 1, time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC)))
	})
}

func TestLifecycleExpirationUnversioned(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, false, false)
		e.setRules(lcRule("Enabled", lcPrefix("logs/"), lcExpireDays(2)))
		e.put("logs/a", "a", lcBase)
		e.put("other/b", "b", lcBase)

		e.runAt(lcDay(4).Add(-time.Second))
		assert.Equal(t, []string{"logs/a", "other/b"}, e.keys())

		e.runAt(lcDay(4))
		assert.Equal(t, []string{"other/b"}, e.keys())

		// a second run finds nothing left to do
		e.run()
		assert.Equal(t, []string{"other/b"}, e.keys())
	})
}

func TestLifecycleExpirationVersioned(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcExpireDays(1)))
		v1 := e.put("key", "one", lcBase)

		e.runAt(lcDay(3))
		require.Equal(t, 1, e.markers())
		assert.True(t, e.exists("key", v1), "the expired version is kept as a noncurrent version")

		// the delete marker is current now and is not expired again
		e.runAt(lcDay(10))
		assert.Equal(t, 1, e.markers())
		assert.Len(t, e.versions(), 2)
	})
}

func TestLifecycleExpirationDate(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, false, false)
		e.setRules(lcRule("Enabled", lcPrefix(""),
			"<Expiration><Date>2030-01-10T00:00:00Z</Date></Expiration>"))
		e.put("before", "x", lcDay(5))
		e.put("after", "x", lcDay(12))

		e.runAt(lcDay(9))
		assert.Equal(t, []string{"after", "before"}, e.keys())

		e.runAt(lcDay(13))
		assert.Empty(t, e.keys(), "an object created after the date is expired as well")
	})
}

func TestLifecycleNoncurrentFromSuccessor(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(5, 0)))
		v1 := e.put("key", "one", lcBase)
		v2 := e.put("key", "two", lcBase.AddDate(0, 0, 9))

		// v1 is old, but has been noncurrent only since v2 was created
		e.runAt(lcDay(8))
		assert.True(t, e.exists("key", v1))

		e.runAt(lcDay(15))
		assert.True(t, e.exists("key", v1))

		e.runAt(lcDay(16))
		assert.False(t, e.exists("key", v1))
		assert.True(t, e.exists("key", v2), "the current version is not touched")
	})
}

func TestLifecycleNewerNoncurrentVersions(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 2)))
		ids := make([]string, 5)
		for i := range ids {
			ids[i] = e.put("key", fmt.Sprint(i), lcBase.AddDate(0, 0, i))
		}

		e.runAt(lcDay(20))
		// v5 is current, v4 and v3 are the two newest noncurrent versions
		for i, id := range ids {
			assert.Equal(t, i >= 2, e.exists("key", id), "version %d", i+1)
		}

		// the count keeps the versions even when they are old
		e.runAt(lcDay(28))
		assert.Len(t, e.versions(), 3)
	})
}

func TestLifecycleExpiredObjectDeleteMarker(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		t.Run("removed in the run that removes the last version", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(
				lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)),
				lcRule("Enabled", lcPrefix(""), lcExpiredMarker))
			e.put("key", "one", lcBase)
			e.del("key", lcBase.AddDate(0, 0, 1))
			e.put("keep", "x", lcBase)

			e.runAt(lcDay(10))
			assert.Equal(t, []string{"keep"}, e.keys())
			assert.Zero(t, e.markers())
		})
		t.Run("kept while a noncurrent version remains", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(
				lcRule("Enabled", lcPrefix(""), lcNoncurrent(30, 0)),
				lcRule("Enabled", lcPrefix(""), lcExpiredMarker))
			e.put("key", "one", lcBase)
			e.del("key", lcBase.AddDate(0, 0, 1))

			e.runAt(lcDay(10))
			assert.Equal(t, 1, e.markers())
			assert.Len(t, e.versions(), 2)

			e.runAt(lcDay(40))
			assert.Empty(t, e.versions())
		})
		t.Run("kept without the rule", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
			e.put("key", "one", lcBase)
			e.del("key", lcBase.AddDate(0, 0, 1))

			e.runAt(lcDay(10))
			assert.Equal(t, 1, e.markers())
			assert.Len(t, e.versions(), 1)
		})
	})
}

func TestLifecycleObjectLock(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		retain := func(mode types.ObjectLockRetentionMode, until time.Time) []byte {
			b, err := json.Marshal(types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until})
			require.NoError(t, err)
			return b
		}

		for _, mode := range []types.ObjectLockRetentionMode{
			types.ObjectLockRetentionModeCompliance,
			types.ObjectLockRetentionModeGovernance,
		} {
			t.Run(string(mode)+" retention", func(t *testing.T) {
				e := newLifecycleEnv(t, mk, true, true)
				e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
				v1 := e.put("key", "one", lcBase)
				require.NoError(t, e.p.PutObjectRetention(context.Background(), e.bucket, "key", v1, retain(mode, lcDay(20))))
				v2 := e.put("key", "two", lcBase.AddDate(0, 0, 1))

				e.runAt(lcDay(10))
				assert.True(t, e.exists("key", v1), "locked version is kept")

				e.runAt(lcDay(19))
				assert.True(t, e.exists("key", v1))

				e.runAt(lcDay(21))
				assert.False(t, e.exists("key", v1), "version is deleted once the retention has passed")
				assert.True(t, e.exists("key", v2))
			})
		}

		t.Run("legal hold", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, true)
			e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
			v1 := e.put("key", "one", lcBase)
			require.NoError(t, e.p.PutObjectLegalHold(context.Background(), e.bucket, "key", v1, true))
			e.put("key", "two", lcBase.AddDate(0, 0, 1))

			e.runAt(time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC))
			assert.True(t, e.exists("key", v1))

			require.NoError(t, e.p.PutObjectLegalHold(context.Background(), e.bucket, "key", v1, false))
			e.run()
			assert.False(t, e.exists("key", v1))
		})

		t.Run("current version gets a delete marker", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, true)
			e.setRules(lcRule("Enabled", lcPrefix(""), lcExpireDays(1)))
			v1 := e.put("key", "one", lcBase)
			require.NoError(t, e.p.PutObjectRetention(context.Background(), e.bucket, "key", v1,
				retain(types.ObjectLockRetentionModeCompliance, lcDay(20))))

			e.runAt(lcDay(5))
			assert.Equal(t, 1, e.markers())
			assert.True(t, e.exists("key", v1))
		})

		t.Run("version without lock next to a locked one", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, true)
			e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
			v1 := e.put("key", "one", lcBase)
			v2 := e.put("key", "two", lcBase.AddDate(0, 0, 1))
			require.NoError(t, e.p.PutObjectRetention(context.Background(), e.bucket, "key", v2,
				retain(types.ObjectLockRetentionModeGovernance, lcDay(20))))
			e.put("key", "three", lcBase.AddDate(0, 0, 2))

			e.runAt(lcDay(10))
			assert.False(t, e.exists("key", v1))
			assert.True(t, e.exists("key", v2))
		})
	})
}

func TestLifecycleFilters(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		tag := func(k, v string) string {
			return "<Tag><Key>" + k + "</Key><Value>" + v + "</Value></Tag>"
		}
		putTagged := func(e *lifecycleEnv, key string, tags map[string]string) {
			e.put(key, "x", lcBase)
			if tags != nil {
				require.NoError(t, e.p.PutObjectTagging(context.Background(), e.bucket, key, "", tags))
			}
		}

		t.Run("tag", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Enabled", "<Filter>"+tag("env", "tmp")+"</Filter>", lcExpireDays(1)))
			putTagged(e, "a", map[string]string{"env": "tmp"})
			putTagged(e, "b", map[string]string{"env": "prod"})
			putTagged(e, "c", nil)

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"b", "c"}, e.keys())
		})
		t.Run("and with prefix and tags", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Enabled", "<Filter><And><Prefix>p/</Prefix>"+tag("a", "1")+tag("b", "2")+"</And></Filter>", lcExpireDays(1)))
			putTagged(e, "p/both", map[string]string{"a": "1", "b": "2"})
			putTagged(e, "p/one", map[string]string{"a": "1"})
			putTagged(e, "q/both", map[string]string{"a": "1", "b": "2"})

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"p/one", "q/both"}, e.keys())
		})
		t.Run("tag of the version", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(lcRule("Enabled", "<Filter>"+tag("env", "tmp")+"</Filter>", lcNoncurrent(1, 0)))
			v1 := e.put("key", "one", lcBase)
			require.NoError(t, e.p.PutObjectTagging(context.Background(), e.bucket, "key", v1, map[string]string{"env": "tmp"}))
			v2 := e.put("key", "two", lcBase.AddDate(0, 0, 1))
			require.NoError(t, e.p.PutObjectTagging(context.Background(), e.bucket, "key", v2, map[string]string{"env": "prod"}))
			v3 := e.put("key", "three", lcBase.AddDate(0, 0, 2))

			e.runAt(lcDay(10))
			assert.False(t, e.exists("key", v1))
			assert.True(t, e.exists("key", v2))
			assert.True(t, e.exists("key", v3))
		})
		t.Run("size greater than", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Enabled", "<Filter><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></Filter>", lcExpireDays(1)))
			e.put("small", strings.Repeat("x", 10), lcBase)
			e.put("big", strings.Repeat("x", 11), lcBase)

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"small"}, e.keys())
		})
		t.Run("size less than", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Enabled", "<Filter><ObjectSizeLessThan>10</ObjectSizeLessThan></Filter>", lcExpireDays(1)))
			e.put("small", strings.Repeat("x", 9), lcBase)
			e.put("big", strings.Repeat("x", 10), lcBase)

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"big"}, e.keys())
		})
		t.Run("legacy prefix", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Enabled", "<Prefix>p/</Prefix>", lcExpireDays(1)))
			e.put("p/a", "x", lcBase)
			e.put("q/a", "x", lcBase)

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"q/a"}, e.keys())
		})
		t.Run("disabled rule", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, false, false)
			e.setRules(lcRule("Disabled", lcPrefix(""), lcExpireDays(1)))
			e.put("a", "x", lcBase)

			e.runAt(lcDay(5))
			assert.Equal(t, []string{"a"}, e.keys())
		})
		t.Run("delete marker matches only rules without size and tag", func(t *testing.T) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(lcRule("Enabled", "<Filter><ObjectSizeGreaterThan>0</ObjectSizeGreaterThan></Filter>", lcNoncurrent(1, 0)))
			e.put("key", "one", lcBase)
			e.del("key", lcBase.AddDate(0, 0, 1))
			e.put("key", "two", lcBase.AddDate(0, 0, 2))

			e.runAt(lcDay(10))
			// the noncurrent version goes, the noncurrent marker does not match
			assert.Equal(t, 1, e.markers())
			assert.Len(t, e.versions(), 2)
		})
	})
}

func TestLifecycleAbortIncompleteMultipartUpload(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, false, false)
		e.setRules(lcRule("Enabled", lcPrefix("up/"),
			"<AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>"))

		start := func(key string, ts time.Time) string {
			res, err := e.p.CreateMultipartUpload(context.Background(), s3response.CreateMultipartUploadInput{
				Bucket: &e.bucket,
				Key:    &key,
			})
			require.NoError(t, err)
			sum := sha256.Sum256([]byte(key))
			dir := e.p.ObjectPath(e.bucket, filepath.Join(MetaTmpMultipartDir, fmt.Sprintf("%x", sum), res.UploadId))
			require.NoError(t, os.Chtimes(dir, ts, ts))
			return dir
		}
		oldDir := start("up/old", lcBase)
		recentDir := start("up/recent", lcDay(8))
		otherDir := start("other/old", lcBase)

		e.runAt(lcDay(10))
		assert.NoDirExists(t, oldDir)
		assert.DirExists(t, recentDir)
		assert.DirExists(t, otherDir, "an upload outside of the prefix is kept")

		e.runAt(lcDay(15))
		assert.NoDirExists(t, recentDir)
		assert.DirExists(t, otherDir)
	})
}

func TestLifecyclePaging(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.p.lifecyclePageSize = 2
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))

		current := map[string]string{}
		for k := 0; k < 6; k++ {
			key := fmt.Sprintf("key%d", k)
			for v := 0; v < 4; v++ {
				current[key] = e.put(key, fmt.Sprint(v), lcBase.AddDate(0, 0, v))
			}
		}
		// a key whose newest version is a delete marker
		e.put("gone", "x", lcBase)
		e.del("gone", lcBase.AddDate(0, 0, 1))

		e.runAt(lcDay(20))
		got := e.versions()
		assert.Len(t, got, 7)
		for key, id := range current {
			assert.Contains(t, got, key+"/"+id)
		}
		assert.Equal(t, 1, e.markers(), "the delete marker is removed only by its own rule")
	})
}

func TestLifecycleShutdown(t *testing.T) {
	t.Run("while waiting for the first run", func(t *testing.T) {
		p, err := New(t.TempDir(), meta.XattrMeta{}, PosixOpts{AbsolutePaths: true, LifecycleInterval: time.Hour})
		require.NoError(t, err)
		require.NotNil(t, p.lifecycleDone)

		finished := make(chan struct{})
		go func() {
			p.Shutdown()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown did not stop the lifecycle goroutine")
		}
		select {
		case <-p.lifecycleDone:
		default:
			t.Fatal("lifecycle goroutine is still running")
		}
	})

	t.Run("disabled by default", func(t *testing.T) {
		p, err := New(t.TempDir(), meta.XattrMeta{}, PosixOpts{AbsolutePaths: true})
		require.NoError(t, err)
		defer p.Shutdown()
		assert.Nil(t, p.lifecycleDone)
	})

	t.Run("runs and stops", func(t *testing.T) {
		e := newLifecycleEnv(t, metaModes(t)["xattr"], false, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcExpireDays(1)))
		e.put("a", "x", lcBase)
		e.clock = lcDay(5)

		e.p.startLifecycle(time.Hour, 0)
		require.Eventually(t, func() bool { return len(e.keys()) == 0 }, 5*time.Second, 10*time.Millisecond)

		e.p.stopLifecycle()
		select {
		case <-e.p.lifecycleDone:
		default:
			t.Fatal("lifecycle goroutine is still running")
		}

		// no run after the stop
		e.put("b", "x", lcBase)
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, []string{"b"}, e.keys())
	})

	t.Run("cancelled context ends a run", func(t *testing.T) {
		e := newLifecycleEnv(t, metaModes(t)["xattr"], false, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcExpireDays(1)))
		e.put("a", "x", lcBase)
		e.clock = lcDay(5)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		e.p.runLifecycle(ctx)
		assert.Equal(t, []string{"a"}, e.keys())
	})
}

func TestLifecycleExpireCurrentSkipsReplacedVersion(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, false, false)
		e.put("key", "new", lcDay(3))

		// the version the listing saw has been replaced since
		b := &lifecycleBucket{name: e.bucket, now: lcDay(10)}
		stale := &lifecycleVersion{id: "", modTime: lcBase, size: 3, current: true}
		var stats lifecycleStats
		e.p.lifecycleExpireCurrent(context.Background(), b, "key", stale, &stats)

		assert.Equal(t, lifecycleStats{raced: 1}, stats)
		assert.Equal(t, []string{"key"}, e.keys())
	})
}

// staleKey lists the bucket and groups it the way a lifecycle run does.
func (e *lifecycleEnv) staleKey(key string) (*lifecycleBucket, *lifecycleKey) {
	e.t.Helper()
	rules, err := e.p.loadLifecycleRules(e.bucket)
	require.NoError(e.t, err)
	b := &lifecycleBucket{name: e.bucket, status: types.BucketVersioningStatusEnabled, now: e.clock, rules: rules}
	res, err := e.p.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: &e.bucket, MaxKeys: aws.Int32(1000)})
	require.NoError(e.t, err)
	for _, g := range groupLifecycleKeys(res) {
		if g.key == key {
			return b, g
		}
	}
	e.t.Fatalf("key %q not listed", key)
	return nil, nil
}

func (e *lifecycleEnv) deleteVersion(key, id string) {
	e.t.Helper()
	_, err := e.p.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &e.bucket, Key: &key, VersionId: &id})
	require.NoError(e.t, err)
}

func (e *lifecycleEnv) retain(key, id string, mode types.ObjectLockRetentionMode, until time.Time) {
	e.t.Helper()
	b, err := json.Marshal(types.ObjectLockRetention{Mode: mode, RetainUntilDate: &until})
	require.NoError(e.t, err)
	require.NoError(e.t, e.p.PutObjectRetention(context.Background(), e.bucket, key, id, b))
}

// versionAttrs returns where the attributes of a noncurrent version are stored.
func (e *lifecycleEnv) versionAttrs(key, id string) (string, string) {
	return filepath.Join(e.p.versioningDir, e.bucket), filepath.Join(genObjVersionKey(e.p.encodeKey(key)), id)
}

// TestLifecycleVersionPromotedByClient deletes the current version after the
// listing: the noncurrent version before it becomes the current one and
// must not be deleted, whether or not it is under retention.
func TestLifecycleVersionPromotedByClient(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(fmt.Sprint("locked=", locked), func(t *testing.T) {
			eachMeta(t, func(t *testing.T, mk metaMaker) {
				e := newLifecycleEnv(t, mk, true, true)
				e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
				v1 := e.put("key", "one", lcBase)
				if locked {
					e.retain("key", v1, types.ObjectLockRetentionModeCompliance, lcDay(20))
				}
				v2 := e.put("key", "two", lcBase.AddDate(0, 0, 1))

				e.clock = lcDay(10)
				b, g := e.staleKey("key")
				e.deleteVersion("key", v2)

				var stats lifecycleStats
				e.p.lifecycleApplyKey(context.Background(), b, g, &stats)
				assert.True(t, e.exists("key", v1), "the version that became current is kept")
				assert.Zero(t, stats.deleted)
				assert.Equal(t, 1, stats.raced)
			})
		})
	}
}

// TestLifecycleMarkerReplacedByClient puts a version over the current delete
// marker after the listing: the marker is not current any more.
func TestLifecycleMarkerReplacedByClient(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcExpiredMarker))
		e.put("key", "one", lcBase)
		e.del("key", lcBase.AddDate(0, 0, 1))
		e.deleteVersion("key", e.versionIDs("key")[0])

		e.clock = lcDay(10)
		b, g := e.staleKey("key")
		v3 := e.put("key", "three", lcDay(9))

		var stats lifecycleStats
		e.p.lifecycleApplyKey(context.Background(), b, g, &stats)
		assert.True(t, e.exists("key", v3))
		assert.Equal(t, 1, e.markers(), "the marker is kept")
		assert.Zero(t, stats.markersRemoved)
	})
}

func (e *lifecycleEnv) versionIDs(key string) []string {
	e.t.Helper()
	var out []string
	for _, v := range e.versions() {
		if k, id, ok := strings.Cut(v, "/"); ok && k == key && !strings.HasSuffix(id, " (marker)") {
			out = append(out, id)
		}
	}
	return out
}

// TestLifecycleCurrentMarkerHidingVersions keeps a marker that hides a
// version the listing did not show.
func TestLifecycleCurrentMarkerHidingVersions(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcExpiredMarker))
		e.put("key", "one", lcBase)
		e.del("key", lcBase.AddDate(0, 0, 1))

		e.clock = lcDay(10)
		b, g := e.staleKey("key")
		// the listing as seen by a page that missed the older version
		g.versions = nil

		var stats lifecycleStats
		e.p.lifecycleApplyKey(context.Background(), b, g, &stats)
		assert.Len(t, e.versions(), 2)
		assert.Zero(t, stats.markersRemoved)
	})
}

// TestLifecycleFailsClosed keeps versions whose lock cannot be read.
func TestLifecycleFailsClosed(t *testing.T) {
	setup := func(t *testing.T, mk metaMaker) (*lifecycleEnv, string) {
		e := newLifecycleEnv(t, mk, true, true)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
		v1 := e.put("key", "one", lcBase)
		e.put("key", "two", lcBase.AddDate(0, 0, 1))
		return e, v1
	}

	for name, attr := range map[string]struct {
		key  string
		data []byte
	}{
		"corrupt retention":      {objectRetentionKey, []byte("not json")},
		"retention without date": {objectRetentionKey, []byte(`{"Mode":"COMPLIANCE"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			eachMeta(t, func(t *testing.T, mk metaMaker) {
				e, v1 := setup(t, mk)
				attrBucket, attrObject := e.versionAttrs("key", v1)
				require.NoError(t, e.p.meta.StoreAttribute(nil, attrBucket, attrObject, attr.key, attr.data))

				e.clock = lcDay(10)
				stats, _ := e.p.lifecycleRunBucket(context.Background(), e.bucket)
				assert.True(t, e.exists("key", v1))
				assert.Equal(t, 1, stats.errors)
				assert.Zero(t, stats.deleted)
			})
		})
	}

	t.Run("missing version", func(t *testing.T) {
		eachMeta(t, func(t *testing.T, mk metaMaker) {
			e, _ := setup(t, mk)
			e.clock = lcDay(10)
			b, _ := e.staleKey("key")

			gone := &lifecycleVersion{id: "01ARZ3NDEKTSV4RRFFQ69G5FAV"}
			var stats lifecycleStats
			assert.True(t, e.p.lifecycleDeleteVersion(context.Background(), b, "key", gone, &stats))
			assert.Zero(t, stats.deleted, "a version that did not exist is not counted")
			assert.Equal(t, 1, stats.raced)
		})
	})
}

// TestLifecycleDeletedVersionsAreCounted counts what a run deletes.
func TestLifecycleDeletedVersionsAreCounted(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
		e.put("key", "one", lcBase)
		e.put("key", "two", lcBase.AddDate(0, 0, 1))
		e.put("key", "three", lcBase.AddDate(0, 0, 2))

		e.clock = lcDay(10)
		stats, ok := e.p.lifecycleRunBucket(context.Background(), e.bucket)
		require.True(t, ok)
		assert.Equal(t, 2, stats.deleted)
		assert.Len(t, e.versions(), 1)
	})
}

// TestLifecycleVanishedVersionMarker removes the version a page ends in
// before the next page is listed.
func TestLifecycleVanishedVersionMarker(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.put("a", "1", lcBase)
		a2 := e.put("a", "2", lcBase.Add(time.Hour))
		e.put("a", "3", lcBase.Add(2*time.Hour))
		e.put("b", "1", lcBase)
		e.put("c", "1", lcBase)

		list := func(keyMarker, versionMarker *string) s3response.ListVersionsResult {
			res, err := e.p.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
				Bucket: &e.bucket, MaxKeys: aws.Int32(2), KeyMarker: keyMarker, VersionIdMarker: versionMarker,
			})
			require.NoError(t, err)
			return res
		}
		page1 := list(nil, nil)
		require.True(t, aws.ToBool(page1.IsTruncated))
		require.Equal(t, a2, aws.ToString(page1.NextVersionIdMarker))

		e.deleteVersion("a", a2)

		var keys []string
		page := list(page1.NextKeyMarker, page1.NextVersionIdMarker)
		for {
			for _, v := range page.Versions {
				keys = append(keys, aws.ToString(v.Key))
			}
			if !aws.ToBool(page.IsTruncated) {
				break
			}
			page = list(page.NextKeyMarker, page.NextVersionIdMarker)
		}
		assert.Contains(t, keys, "b")
		assert.Contains(t, keys, "c")
	})
}

// TestLifecycleHugeDays does not expire anything for day counts that
// overflow a duration.
func TestLifecycleHugeDays(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(
			lcRule("Enabled", lcPrefix("a/"), lcExpireDays(2147483647)),
			lcRule("Enabled", lcPrefix("b/"), lcNoncurrent(200000, 0)))
		e.put("a/key", "x", lcBase)
		e.put("b/key", "one", lcBase)
		e.put("b/key", "two", lcBase.AddDate(0, 0, 1))

		e.runAt(lcDay(20))
		assert.Len(t, e.versions(), 3)
		assert.Zero(t, e.markers())
	})
}

// TestLifecycleMarkerHiddenVersionAge ages a version from the creation of the
// delete marker even when the marker kept the last modified time of the
// version it hides.
func TestLifecycleMarkerHiddenVersionAge(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(5, 0)))

		now := time.Now().UTC()
		old := now.AddDate(0, 0, -100)
		v1 := e.put("key", "one", old)
		marker := e.del("key", old)
		// the marker was created now but kept the old time, as if the
		// gateway could not set it
		e.setTime("key", marker, old)
		require.NoError(t, os.Chtimes(e.p.ObjectPath(e.bucket, "key"), old, old))

		e.runAt(now.Add(time.Hour))
		assert.True(t, e.exists("key", v1), "the version is hidden for no longer than the marker exists")

		e.runAt(now.AddDate(0, 0, 7))
		assert.False(t, e.exists("key", v1))
	})
}

func TestLifecycleExpiredMarkerByExpiration(t *testing.T) {
	for name, expiration := range map[string]string{
		"days": lcExpireDays(1),
		"date": "<Expiration><Date>2030-01-05T00:00:00Z</Date></Expiration>",
	} {
		t.Run(name, func(t *testing.T) {
			eachMeta(t, func(t *testing.T, mk metaMaker) {
				e := newLifecycleEnv(t, mk, true, false)
				e.setRules(
					lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)),
					lcRule("Enabled", lcPrefix(""), expiration))
				e.put("key", "one", lcBase)
				e.del("key", lcBase.AddDate(0, 0, 1))

				// the version goes in the first run, the marker that hides
				// nothing in the same one
				e.runAt(lcDay(10))
				assert.Empty(t, e.versions())
			})
		})
	}

	t.Run("not due", func(t *testing.T) {
		eachMeta(t, func(t *testing.T, mk metaMaker) {
			e := newLifecycleEnv(t, mk, true, false)
			e.setRules(
				lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)),
				lcRule("Enabled", lcPrefix(""), lcExpireDays(30)))
			e.put("key", "one", lcBase)
			e.del("key", lcBase.AddDate(0, 0, 1))

			e.runAt(lcDay(10))
			assert.Equal(t, 1, e.markers())
		})
	})
}

// TestLifecycleVersionReplacedAfterListing keeps an entry whose id is the
// listed one but which is not: the null version of a suspended bucket is
// replaced by a new one under the same id.
func TestLifecycleVersionReplacedAfterListing(t *testing.T) {
	for name, change := range map[string]func(t *testing.T, path string){
		"mtime": func(t *testing.T, path string) {
			require.NoError(t, os.Chtimes(path, lcDay(9), lcDay(9)))
		},
		"size": func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("replaced content"), 0644))
			require.NoError(t, os.Chtimes(path, lcBase, lcBase))
		},
	} {
		t.Run(name, func(t *testing.T) {
			eachMeta(t, func(t *testing.T, mk metaMaker) {
				e := newLifecycleEnv(t, mk, true, false)
				e.setRules(lcRule("Enabled", lcPrefix(""), lcNoncurrent(1, 0)))
				v1 := e.put("key", "one", lcBase)
				e.put("key", "two", lcBase.AddDate(0, 0, 1))

				e.clock = lcDay(10)
				b, g := e.staleKey("key")
				attrBucket, attrObject := e.versionAttrs("key", v1)
				change(t, filepath.Join(attrBucket, attrObject))

				var stats lifecycleStats
				e.p.lifecycleApplyKey(context.Background(), b, g, &stats)
				assert.True(t, e.exists("key", v1))
				assert.Zero(t, stats.deleted)
				assert.Equal(t, 1, stats.raced)
			})
		})
	}
}

// TestDeleteVersionWaitsForPublishLock serializes the removal of a version
// by id, which can move another version to the object path, with the
// operations that hold the publish lock of the key, such as the lifecycle
// engine.
func TestDeleteVersionWaitsForPublishLock(t *testing.T) {
	eachMeta(t, func(t *testing.T, mk metaMaker) {
		e := newLifecycleEnv(t, mk, true, false)
		v1 := e.put("key", "one", lcBase)
		v2 := e.put("key", "two", lcBase.AddDate(0, 0, 1))

		unlock, err := e.p.lockObjectPublish(context.Background(), e.bucket, "key")
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			_, err := e.p.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: &e.bucket, Key: aws.String("key"), VersionId: &v2,
			})
			done <- err
		}()
		select {
		case <-done:
			t.Fatal("version was deleted while the publish lock was held")
		case <-time.After(100 * time.Millisecond):
		}

		unlock()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("delete did not continue after the lock was released")
		}
		assert.Equal(t, []string{"key/" + v1}, e.versions())
	})
}
