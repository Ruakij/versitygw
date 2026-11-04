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

package gwcli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		interval    time.Duration
		start       string
		concurrency int
		want        time.Duration
		wantErr     string
	}{
		{"defaults", 24 * time.Hour, "00:00", 4, 0, ""},
		{"disabled", 0, "00:00", 4, 0, ""},
		{"start with minutes", 6 * time.Hour, "03:30", 1, 3*time.Hour + 30*time.Minute, ""},
		{"start with seconds", 6 * time.Hour, "23:59:59", 8, 23*time.Hour + 59*time.Minute + 59*time.Second, ""},
		{"negative interval", -time.Hour, "00:00", 4, 0, "interval"},
		{"start out of range", time.Hour, "24:00", 4, 0, "lifecycle start"},
		{"start not a time", time.Hour, "midnight", 4, 0, "lifecycle start"},
		{"empty start", time.Hour, "", 4, 0, "lifecycle start"},
		{"zero concurrency", time.Hour, "00:00", 0, 0, "concurrency"},
		{"negative concurrency", time.Hour, "00:00", -2, 0, "concurrency"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lifecycleInterval, lifecycleStart, lifecycleConcurrency = tt.interval, tt.start, tt.concurrency
			got, err := validateLifecycle()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
