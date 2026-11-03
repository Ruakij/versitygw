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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fillDeleteMarker writes content into the delete marker file at path, as
// versions of the gateway that kept the content of the hidden object did.
func fillDeleteMarker(t *testing.T, path, content string) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func truncateTestMarkers(t *testing.T, p *Posix, dryRun bool) (TruncateDeleteMarkersResult, map[string]string) {
	t.Helper()

	skipped := map[string]string{}
	res, err := TruncateDeleteMarkers(context.Background(), TruncateDeleteMarkersOpts{
		Root:          testRoot(t, p),
		VersioningDir: p.versioningDir,
		Meta:          p.meta,
		DryRun:        dryRun,
		Skipped:       func(path, reason string) { skipped[path] = reason },
		Failed:        func(path string, err error) { t.Errorf("%v: %v", path, err) },
	})
	require.NoError(t, err)
	return res, skipped
}

func TestTruncateDeleteMarkers(t *testing.T) {
	ctx := context.Background()
	bucket, key := "bucket", "dir/object"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)

			// version 1 is hidden by marker 1, which version 2 hid
			// again with marker 2: the live marker and the snapshot of
			// marker 1 have the content of the version they hide
			vId1 := putVersion(t, p, bucket, key, "content-1")
			delVId1 := deleteVersion(t, p, bucket, key)
			vId2 := putVersion(t, p, bucket, key, "content-two")
			delVId2 := deleteVersion(t, p, bucket, key)

			vPath := p.genObjVersionPath(bucket, key)
			live := filepath.Join(testRoot(t, p), bucket, key)
			snapshot1 := filepath.Join(vPath, delVId1)
			fillDeleteMarker(t, snapshot1, "content-1")
			fillDeleteMarker(t, live, "content-two")

			list := func() []time.Time {
				res, err := p.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, MaxKeys: aws.Int32(100)})
				require.NoError(t, err)
				require.Len(t, res.DeleteMarkers, 2)
				var times []time.Time
				for _, m := range res.DeleteMarkers {
					times = append(times, *m.LastModified)
				}
				return times
			}
			markerTimes := list()

			t.Run("dry run", func(t *testing.T) {
				res, skipped := truncateTestMarkers(t, p, true)
				assert.Equal(t, DeleteMarkerStats{Count: 2, Bytes: 9 + 11}, res.Truncated)
				assert.Empty(t, skipped)
				assert.Zero(t, res.Errors)

				assert.Equal(t, int64(11), statSize(t, live))
				assert.Equal(t, int64(9), statSize(t, snapshot1))
			})

			t.Run("truncate", func(t *testing.T) {
				res, skipped := truncateTestMarkers(t, p, false)
				assert.Equal(t, DeleteMarkerStats{Count: 2, Bytes: 9 + 11}, res.Truncated)
				assert.Empty(t, skipped)
				assert.Zero(t, res.Errors)

				assert.Zero(t, statSize(t, live))
				assert.Zero(t, statSize(t, snapshot1))
				assert.Equal(t, int64(9), statSize(t, filepath.Join(vPath, vId1)))
				assert.Equal(t, int64(11), statSize(t, filepath.Join(vPath, vId2)))
				assert.Equal(t, "content-1", getVersionBody(t, p, bucket, key, vId1))
				assert.Equal(t, "content-two", getVersionBody(t, p, bucket, key, vId2))
				assert.Equal(t, markerTimes, list())

				// the live marker still undeletes the previous version
				_, err := p.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, VersionId: &delVId2})
				require.NoError(t, err)
				assert.Equal(t, "content-two", getVersionBody(t, p, bucket, key, ""))
			})

			t.Run("nothing left", func(t *testing.T) {
				res, skipped := truncateTestMarkers(t, p, false)
				assert.Zero(t, res.Truncated)
				assert.Empty(t, res.Skipped)
				assert.Empty(t, skipped)
			})
		})
	}
}

func TestTruncateDeleteMarkersSkipped(t *testing.T) {
	bucket := "bucket"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)
			objPath := func(key string) string {
				return filepath.Join(testRoot(t, p), bucket, key)
			}

			// the version hidden by the marker was deleted
			vId := putVersion(t, p, bucket, "no-version", "content")
			deleteVersion(t, p, bucket, "no-version")
			fillDeleteMarker(t, objPath("no-version"), "content")
			require.NoError(t, os.Remove(filepath.Join(p.genObjVersionPath(bucket, "no-version"), vId)))

			// the marker size differs from the one of the version
			putVersion(t, p, bucket, "other-size", "content")
			deleteVersion(t, p, bucket, "other-size")
			fillDeleteMarker(t, objPath("other-size"), "other content")

			// the versions are deleted but for a marker, which isn't
			// the content of the other marker
			vId = putVersion(t, p, bucket, "markers-only", "content")
			delVId := deleteVersion(t, p, bucket, "markers-only")
			vId2 := putVersion(t, p, bucket, "markers-only", "content")
			deleteVersion(t, p, bucket, "markers-only")
			versionsPath := p.genObjVersionPath(bucket, "markers-only")
			require.NoError(t, os.Remove(filepath.Join(versionsPath, vId)))
			require.NoError(t, os.Remove(filepath.Join(versionsPath, vId2)))
			snapshot := filepath.Join(versionsPath, delVId)
			fillDeleteMarker(t, snapshot, "content")
			fillDeleteMarker(t, objPath("markers-only"), "content")

			// an object that is not a marker
			putVersion(t, p, bucket, "object", "content")

			res, skipped := truncateTestMarkers(t, p, false)
			assert.Zero(t, res.Truncated)
			assert.Zero(t, res.Errors)
			assert.Equal(t, map[string]string{
				objPath("no-version"):   SkipNoVersions,
				objPath("other-size"):   SkipNoMatchingVersion,
				snapshot:                SkipNoVersions,
				objPath("markers-only"): SkipNoMatchingVersion,
			}, skipped)
			assert.Equal(t, DeleteMarkerStats{Count: 2, Bytes: 14}, res.Skipped[SkipNoVersions])
			assert.Equal(t, DeleteMarkerStats{Count: 2, Bytes: 13 + 7}, res.Skipped[SkipNoMatchingVersion])

			assert.Equal(t, int64(7), statSize(t, objPath("no-version")))
			assert.Equal(t, int64(13), statSize(t, objPath("other-size")))
			assert.Equal(t, int64(7), statSize(t, snapshot))
			assert.Equal(t, int64(7), statSize(t, objPath("markers-only")))
			assert.Equal(t, int64(7), statSize(t, objPath("object")))
		})
	}
}

// TestTruncateDeleteMarkersSyncsVersionFirst checks that the version that
// proves the content exists is flushed while the marker still has it.
func TestTruncateDeleteMarkersSyncsVersionFirst(t *testing.T) {
	bucket, key := "bucket", "object"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)
			vId := putVersion(t, p, bucket, key, "content")
			deleteVersion(t, p, bucket, key)
			live := filepath.Join(testRoot(t, p), bucket, key)
			fillDeleteMarker(t, live, "content")

			origFile := syncFile
			t.Cleanup(func() { syncFile = origFile })
			var files []string
			syncFile = func(f *os.File) error {
				files = append(files, f.Name())
				assert.Equal(t, int64(7), statSize(t, live))
				return origFile(f)
			}

			truncateTestMarkers(t, p, true)
			assert.Empty(t, files)

			res, _ := truncateTestMarkers(t, p, false)
			assert.Equal(t, int64(1), res.Truncated.Count)
			assert.Equal(t, []string{filepath.Join(p.genObjVersionPath(bucket, key), vId)}, files)
			assert.Zero(t, statSize(t, live))
		})
	}
}

func TestDeleteMarkerMigrationAtStartup(t *testing.T) {
	bucket, key := "bucket", "object"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)
			<-p.migrationDone
			done := filepath.Join(p.versioningDir, deleteMarkersTruncatedFile)
			require.FileExists(t, done, "a run without delete markers finishes")
			require.NoError(t, os.Remove(done))

			putVersion(t, p, bucket, key, "content")
			deleteVersion(t, p, bucket, key)
			live := filepath.Join(testRoot(t, p), bucket, key)
			fillDeleteMarker(t, live, "content")

			p.startDeleteMarkerMigration()
			<-p.migrationDone
			assert.Zero(t, statSize(t, live))
			assert.FileExists(t, done)

			fillDeleteMarker(t, live, "content")
			p.startDeleteMarkerMigration()
			<-p.migrationDone
			assert.Equal(t, int64(7), statSize(t, live), "a finished migration is not repeated")
		})
	}
}
