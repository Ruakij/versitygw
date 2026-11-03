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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/versity/versitygw/backend/meta"
)

// Reasons a delete marker with content is not truncated.
const (
	// SkipNoVersions means the versioning directory holds no version of
	// the marker key.
	SkipNoVersions = "no version of the key in the versioning directory"
	// SkipNoMatchingVersion means none of the versions of the marker key
	// has the size of the marker.
	SkipNoMatchingVersion = "no version of the same size in the versioning directory"
)

// DeleteMarkerStats counts delete marker files and their size.
type DeleteMarkerStats struct {
	Count int64
	Bytes int64
}

func (s *DeleteMarkerStats) add(size int64) {
	s.Count++
	s.Bytes += size
}

// TruncateDeleteMarkersResult is the outcome of TruncateDeleteMarkers.
type TruncateDeleteMarkersResult struct {
	// Truncated are the markers truncated, or with DryRun the ones that
	// would be.
	Truncated DeleteMarkerStats
	// Skipped are the markers left as is by reason.
	Skipped map[string]DeleteMarkerStats
	// Errors is the number of files that couldn't be processed.
	Errors int64
}

// TruncateDeleteMarkersOpts configures TruncateDeleteMarkers.
type TruncateDeleteMarkersOpts struct {
	// Root is the posix root directory, its directories are the buckets.
	Root string
	// VersioningDir is the versioning directory of the gateway.
	VersioningDir string
	// Meta is the metadata storer of the gateway. It resolves bucket names
	// below Root and absolute bucket paths as given.
	Meta meta.MetadataStorer
	// DryRun only reports what would be truncated.
	DryRun bool
	// Skipped is called with the path and reason of each marker that is
	// left as is. It is optional.
	Skipped func(path, reason string)
	// Failed is called with the path and error of each file that couldn't
	// be processed. It is optional.
	Failed func(path string, err error)
}

// TruncateDeleteMarkers empties the delete markers that keep the content of
// the object they hide, as created by versions of the gateway that didn't
// empty them. It visits the buckets below Root and the versioning directory.
//
// A marker is truncated only if the versioning directory holds a version of
// the same key that isn't a delete marker and has the size of the marker,
// which proves the content still exists. The file is checked and truncated
// through one open file, so an object that replaces the marker at the same
// time is never truncated. It is safe to run while the gateway is running.
func TruncateDeleteMarkers(ctx context.Context, opts TruncateDeleteMarkersOpts) (TruncateDeleteMarkersResult, error) {
	t := &markerTruncator{
		TruncateDeleteMarkersOpts: opts,
		res: TruncateDeleteMarkersResult{
			Skipped: map[string]DeleteMarkerStats{},
		},
	}
	if opts.Root == "" || opts.VersioningDir == "" || opts.Meta == nil {
		return t.res, errors.New("root directory, versioning directory and metadata storer are required")
	}

	err := t.walkBuckets(ctx, opts.Root, t.bucketFile)
	if err != nil {
		return t.res, err
	}
	err = t.walkBuckets(ctx, opts.VersioningDir, t.versionFile)
	return t.res, err
}

type markerTruncator struct {
	TruncateDeleteMarkersOpts
	res TruncateDeleteMarkersResult
}

// walkBuckets calls fn for each regular file with content below the bucket
// directories of dir. bucket is the directory name and rel the path of the
// file in it.
func (t *markerTruncator) walkBuckets(ctx context.Context, dir string, fn func(bucket, rel string)) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read directory: %w", err)
	}

	for _, ent := range ents {
		if !ent.IsDir() || ent.Name() == objLockDir {
			continue
		}
		bucket := ent.Name()
		bucketPath := filepath.Join(dir, bucket)

		err := filepath.WalkDir(bucketPath, func(path string, d fs.DirEntry, err error) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				t.fail(path, fmt.Errorf("walk: %w", err))
				return nil
			}
			if d.IsDir() {
				if d.Name() == MetaTmpDir && filepath.Dir(path) == bucketPath {
					return fs.SkipDir
				}
				return nil
			}
			if !d.Type().IsRegular() {
				return nil
			}
			fi, err := d.Info()
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				t.fail(path, err)
				return nil
			}
			if fi.Size() == 0 {
				return nil
			}
			rel, err := filepath.Rel(bucketPath, path)
			if err != nil {
				t.fail(path, err)
				return nil
			}
			fn(bucket, rel)
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// markerFile locates a candidate delete marker.
type markerFile struct {
	// path is the filesystem path of the file.
	path string
	// bucket and object address the file in the metadata storer.
	bucket, object string
	// versionBucket is the bucket directory in the versioning directory.
	versionBucket string
	// versionKey is the directory below versionBucket that holds the
	// versions of the key of the file.
	versionKey string
	// self is the name of the file in versionKey, if it is there.
	self string
}

// bucketFile handles a file of a bucket, whose versions are in the
// versioning directory.
func (t *markerTruncator) bucketFile(bucket, rel string) {
	object := filepath.ToSlash(rel)
	t.process(markerFile{
		path:          filepath.Join(t.Root, bucket, rel),
		bucket:        bucket,
		object:        object,
		versionBucket: filepath.Join(t.VersioningDir, bucket),
		versionKey:    genObjVersionKey(object),
	})
}

// versionFile handles a file of the versioning directory, whose versions are
// the files next to it.
func (t *markerTruncator) versionFile(bucket, rel string) {
	versionBucket := filepath.Join(t.VersioningDir, bucket)
	t.process(markerFile{
		path:          filepath.Join(versionBucket, rel),
		bucket:        versionBucket,
		object:        rel,
		versionBucket: versionBucket,
		versionKey:    filepath.Dir(rel),
		self:          filepath.Base(rel),
	})
}

// process truncates the file if it is a delete marker with content and a
// version of the same size exists.
func (t *markerTruncator) process(m markerFile) {
	f, err := os.Open(m.path)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		t.fail(m.path, err)
		return
	}
	defer f.Close()

	size, err := t.markerSize(f, m)
	if err != nil {
		t.fail(m.path, err)
		return
	}
	if size == 0 {
		return
	}

	reason, version, err := t.checkVersions(m, size)
	if err != nil {
		t.fail(m.path, err)
		return
	}
	if reason != "" {
		stats := t.res.Skipped[reason]
		stats.add(size)
		t.res.Skipped[reason] = stats
		if t.Skipped != nil {
			t.Skipped(m.path, reason)
		}
		return
	}

	if !t.DryRun {
		done, err := t.truncate(f, m, version)
		if err != nil {
			t.fail(m.path, err)
		}
		if !done {
			return
		}
	}
	t.res.Truncated.add(size)
}

// markerSize returns the size of the open file if it is a delete marker with
// content, otherwise 0.
func (t *markerTruncator) markerSize(f *os.File, m markerFile) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 {
		return 0, nil
	}

	_, err = t.Meta.RetrieveAttribute(f, m.bucket, m.object, deleteMarkerKey)
	if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get delete marker attribute: %w", err)
	}

	// Metadata that isn't stored on the file, like a sidecar, can belong
	// to a file that replaced this one.
	cur, err := os.Stat(m.path)
	if err != nil || !os.SameFile(fi, cur) {
		return 0, nil
	}
	return fi.Size(), nil
}

// checkVersions returns the reason no version of the key of m proves the
// content of a marker of the given size exists, or "" and the path of the
// version if there is one. A version proves it if it has the size and is not
// a delete marker.
func (t *markerTruncator) checkVersions(m markerFile, size int64) (string, string, error) {
	ents, err := os.ReadDir(filepath.Join(m.versionBucket, m.versionKey))
	if errors.Is(err, fs.ErrNotExist) {
		return SkipNoVersions, "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("read version directory: %w", err)
	}

	reason := SkipNoVersions
	for _, ent := range ents {
		if ent.Name() == m.self || !ent.Type().IsRegular() {
			continue
		}
		reason = SkipNoMatchingVersion

		fi, err := ent.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", "", err
		}
		if fi.Size() != size {
			continue
		}

		_, err = t.Meta.RetrieveAttribute(nil, m.versionBucket, filepath.Join(m.versionKey, ent.Name()), deleteMarkerKey)
		if errors.Is(err, meta.ErrNoSuchKey) {
			return "", filepath.Join(m.versionBucket, m.versionKey, ent.Name()), nil
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", "", fmt.Errorf("get delete marker attribute of version %v: %w", ent.Name(), err)
		}
	}
	return reason, "", nil
}

// truncate empties the file of m, which is the open file f and a delete
// marker, after the version at the given path is flushed. It returns false
// if the file was replaced in the meantime. The modification time, the last
// modified time of the marker, is kept.
func (t *markerTruncator) truncate(f *os.File, m markerFile, version string) (bool, error) {
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}

	// f can't be written to if the file isn't writable, which is reported
	// as an error of the file instead of failing to read it.
	w, err := os.OpenFile(m.path, os.O_WRONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer w.Close()

	wi, err := w.Stat()
	if err != nil {
		return false, err
	}
	if !os.SameFile(fi, wi) || wi.Size() != fi.Size() {
		return false, nil
	}
	size, err := t.markerSize(w, m)
	if err != nil || size == 0 {
		return false, err
	}

	// The version holds the content from now on.
	if err := syncObjVersion(version, m.versionBucket); err != nil {
		return false, err
	}

	if err := w.Truncate(0); err != nil {
		return false, err
	}

	cur, err := os.Stat(m.path)
	if err == nil && os.SameFile(wi, cur) {
		err = os.Chtimes(m.path, time.Now(), fi.ModTime())
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return true, fmt.Errorf("restore modification time: %w", err)
	}
	return true, nil
}

func (t *markerTruncator) fail(path string, err error) {
	t.res.Errors++
	if t.Failed != nil {
		t.Failed(path, err)
	}
}
