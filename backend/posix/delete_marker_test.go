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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// newMarkerTestPosix creates a Posix backend with a versioning enabled bucket
// named "bucket".
func newMarkerTestPosix(t *testing.T, mkMeta func(t *testing.T) (meta.MetadataStorer, PosixOpts)) *Posix {
	t.Helper()

	p := newVersionedTestPosix(t, mkMeta)
	createTestBucket(t, p, "bucket")
	err := p.PutBucketVersioning(context.Background(), "bucket", types.BucketVersioningStatusEnabled)
	require.NoError(t, err)
	return p
}

func putVersion(t *testing.T, p *Posix, bucket, key, body string) string {
	t.Helper()

	res, err := p.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          strings.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	require.NoError(t, err)
	return res.VersionID
}

func getVersionBody(t *testing.T, p *Posix, bucket, key, versionId string) string {
	t.Helper()

	in := &s3.GetObjectInput{Bucket: &bucket, Key: &key}
	if versionId != "" {
		in.VersionId = &versionId
	}
	out, err := p.GetObject(context.Background(), in)
	require.NoError(t, err)
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	return string(body)
}

func deleteVersion(t *testing.T, p *Posix, bucket, key string) string {
	t.Helper()

	out, err := p.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err)
	require.NotNil(t, out.DeleteMarker)
	require.True(t, *out.DeleteMarker)
	return aws.ToString(out.VersionId)
}

func testRoot(t *testing.T, p *Posix) string {
	t.Helper()

	root, err := filepath.Abs(p.rootPath())
	require.NoError(t, err)
	return root
}

func statSize(t *testing.T, path string) int64 {
	t.Helper()

	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

func TestDeleteMarkerIsEmpty(t *testing.T) {
	ctx := context.Background()
	bucket, key := "bucket", "dir/object"
	const body = "some object content"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)

			t.Run("marker file is empty and hides the object", func(t *testing.T) {
				vId := putVersion(t, p, bucket, key, body)
				delVId := deleteVersion(t, p, bucket, key)
				assert.NotEqual(t, vId, delVId)

				assert.Zero(t, statSize(t, p.ObjectPath(bucket, key)))
				isDel, err := p.isObjDeleteMarker(bucket, key)
				assert.NoError(t, err)
				assert.True(t, isDel)

				_, err = p.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
				assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrNoSuchKey))
				_, err = p.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
				assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrNoSuchKey))
				_, err = p.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, VersionId: &delVId})
				assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrMethodNotAllowed))
				_, err = p.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key, VersionId: &delVId})
				assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrMethodNotAllowed))

				assert.Equal(t, body, getVersionBody(t, p, bucket, key, vId))
				assert.Equal(t, int64(len(body)), statSize(t, p.genObjVersionPath(bucket, key)+"/"+vId))

				res, err := p.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, MaxKeys: aws.Int32(100)})
				require.NoError(t, err)
				require.Len(t, res.DeleteMarkers, 1)
				assert.Equal(t, delVId, aws.ToString(res.DeleteMarkers[0].VersionId))
				require.Len(t, res.Versions, 1)
				assert.Equal(t, vId, aws.ToString(res.Versions[0].VersionId))
				assert.Equal(t, int64(len(body)), aws.ToInt64(res.Versions[0].Size))
			})

			t.Run("deleting the marker restores the object", func(t *testing.T) {
				key := "restore"
				vId := putVersion(t, p, bucket, key, body)
				delVId := deleteVersion(t, p, bucket, key)

				out, err := p.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, VersionId: &delVId})
				require.NoError(t, err)
				assert.True(t, aws.ToBool(out.DeleteMarker))

				assert.Equal(t, body, getVersionBody(t, p, bucket, key, ""))
				assert.Equal(t, int64(len(body)), statSize(t, p.ObjectPath(bucket, key)))
				head, err := p.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
				require.NoError(t, err)
				assert.Equal(t, vId, aws.ToString(head.VersionId))
				assert.Equal(t, int64(len(body)), aws.ToInt64(head.ContentLength))
			})

			t.Run("put over a marker snapshots an empty marker", func(t *testing.T) {
				key := "reput"
				vId := putVersion(t, p, bucket, key, body)
				delVId := deleteVersion(t, p, bucket, key)
				newVId := putVersion(t, p, bucket, key, "new")

				vPath := p.genObjVersionPath(bucket, key)
				assert.Zero(t, statSize(t, vPath+"/"+delVId))
				assert.Equal(t, int64(len(body)), statSize(t, vPath+"/"+vId))
				assert.Equal(t, "new", getVersionBody(t, p, bucket, key, ""))
				assert.Equal(t, "new", getVersionBody(t, p, bucket, key, newVId))
				assert.Equal(t, body, getVersionBody(t, p, bucket, key, vId))

				res, err := p.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, Prefix: &key, MaxKeys: aws.Int32(100)})
				require.NoError(t, err)
				require.Len(t, res.DeleteMarkers, 1)
				assert.Equal(t, delVId, aws.ToString(res.DeleteMarkers[0].VersionId))
				assert.Len(t, res.Versions, 2)
			})

			t.Run("multi delete empties the marker", func(t *testing.T) {
				key := "multi"
				vId := putVersion(t, p, bucket, key, body)

				res, err := p.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: &bucket,
					Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: &key}}},
				})
				require.NoError(t, err)
				require.Len(t, res.Deleted, 1)
				assert.True(t, aws.ToBool(res.Deleted[0].DeleteMarker))

				assert.Zero(t, statSize(t, p.ObjectPath(bucket, key)))
				assert.Equal(t, body, getVersionBody(t, p, bucket, key, vId))
			})

			t.Run("directory object", func(t *testing.T) {
				key := "dirobj/"
				putVersion(t, p, bucket, key, "")
				deleteVersion(t, p, bucket, key)

				fi, err := os.Stat(p.ObjectPath(bucket, key))
				require.NoError(t, err)
				assert.True(t, fi.IsDir())
				isDel, err := p.isObjDeleteMarker(bucket, key)
				assert.NoError(t, err)
				assert.True(t, isDel)
			})
		})
	}
}

func TestDeleteMarkerIsEmptyVersioningSuspended(t *testing.T) {
	ctx := context.Background()
	bucket := "bucket"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)

			// the version created while enabled is kept next to the null marker
			key := "enabled-then-suspended"
			vId := putVersion(t, p, bucket, key, "content")
			err := p.PutBucketVersioning(ctx, bucket, types.BucketVersioningStatusSuspended)
			require.NoError(t, err)

			delVId := deleteVersion(t, p, bucket, key)
			assert.Equal(t, nullVersionId, delVId)
			assert.Zero(t, statSize(t, p.ObjectPath(bucket, key)))
			assert.Equal(t, "content", getVersionBody(t, p, bucket, key, vId))

			// the null version is replaced by the marker
			key = "null"
			putVersion(t, p, bucket, key, "content")
			delVId = deleteVersion(t, p, bucket, key)
			assert.Equal(t, nullVersionId, delVId)
			assert.Zero(t, statSize(t, p.ObjectPath(bucket, key)))

			_, err = p.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
			assert.ErrorIs(t, err, s3err.GetAPIError(s3err.ErrNoSuchKey))
			res, err := p.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, Prefix: &key, MaxKeys: aws.Int32(100)})
			require.NoError(t, err)
			assert.Len(t, res.DeleteMarkers, 1)
			assert.Empty(t, res.Versions)
		})
	}
}

// TestDeleteMarkerSyncsVersionBeforeEmptying checks that the version copy is
// flushed, with its directories, while the object still holds its content and
// is no delete marker, and that a failed flush fails the delete without
// losing data.
func TestDeleteMarkerSyncsVersionBeforeEmptying(t *testing.T) {
	bucket, key := "bucket", "dir/object"
	const body = "some object content"

	for name, mkMeta := range metaModes(t) {
		t.Run(name, func(t *testing.T) {
			p := newMarkerTestPosix(t, mkMeta)
			vId := putVersion(t, p, bucket, key, body)

			live := filepath.Join(testRoot(t, p), bucket, key)
			versionPath := filepath.Join(p.genObjVersionPath(bucket, key), vId)

			t.Run("flushed before emptying", func(t *testing.T) {
				origFile, origDir := syncFile, syncDir
				t.Cleanup(func() { syncFile, syncDir = origFile, origDir })

				var files, dirs []string
				syncFile = func(f *os.File) error {
					files = append(files, f.Name())
					// the object still holds its content, unmarked
					assert.Equal(t, int64(len(body)), statSize(t, live))
					isDel, err := p.isObjDeleteMarker(bucket, key)
					assert.NoError(t, err)
					assert.False(t, isDel)
					return origFile(f)
				}
				syncDir = func(dir string) error {
					dirs = append(dirs, dir)
					return origDir(dir)
				}

				deleteVersion(t, p, bucket, key)

				assert.Equal(t, []string{versionPath}, files)
				var want []string
				for dir := filepath.Dir(versionPath); ; dir = filepath.Dir(dir) {
					want = append(want, dir)
					if dir == filepath.Join(p.versioningDir, bucket) {
						break
					}
				}
				assert.Equal(t, want, dirs)
				assert.Zero(t, statSize(t, live))
			})

			t.Run("failed flush keeps the content", func(t *testing.T) {
				key := "failing"
				putVersion(t, p, bucket, key, body)

				origFile := syncFile
				t.Cleanup(func() { syncFile = origFile })
				syncFile = func(*os.File) error { return errors.New("injected fsync failure") }

				_, err := p.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
				assert.Error(t, err)

				live := filepath.Join(testRoot(t, p), bucket, key)
				assert.Equal(t, int64(len(body)), statSize(t, live))
				isDel, err := p.isObjDeleteMarker(bucket, key)
				assert.NoError(t, err)
				assert.False(t, isDel)
			})
		})
	}
}
