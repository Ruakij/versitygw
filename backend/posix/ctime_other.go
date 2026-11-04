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

//go:build !linux && !darwin && !freebsd

package posix

import (
	"os"
	"time"
)

// fileChangeTime returns the time the inode of the file last changed, which
// is not known on this platform: the last modified time stands in for it, so
// a delete marker whose time could not be set is aged by the time of the
// version it hides.
func fileChangeTime(fi os.FileInfo) time.Time {
	return fi.ModTime()
}
