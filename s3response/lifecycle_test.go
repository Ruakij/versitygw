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

package s3response

import (
	"encoding/xml"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/versity/versitygw/s3err"
)

func lifecycleRule(inner string) string {
	return "<Rule><Status>Enabled</Status>" + inner + "</Rule>"
}

func lifecycleConfig(rules ...string) string {
	return `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		strings.Join(rules, "") + "</LifecycleConfiguration>"
}

func TestLifecycleConfiguration_Validate(t *testing.T) {
	const (
		filterAll = "<Filter></Filter>"
		expire    = "<Expiration><Days>1</Days></Expiration>"
	)

	manyRules := make([]string, maxLifecycleRules+1)
	for i := range manyRules {
		manyRules[i] = lifecycleRule(fmt.Sprintf("<ID>r%d</ID>%s%s", i, filterAll, expire))
	}

	tests := []struct {
		name string
		xml  string
		err  error
	}{
		{"expiration days", lifecycleConfig(lifecycleRule(filterAll + expire)), nil},
		{"legacy prefix", lifecycleConfig(lifecycleRule("<Prefix>a/</Prefix>" + expire)), nil},
		{"empty legacy prefix", lifecycleConfig(lifecycleRule("<Prefix></Prefix>" + expire)), nil},
		{"disabled", lifecycleConfig(`<Rule><Status>Disabled</Status>` + filterAll + expire + `</Rule>`), nil},
		{"expiration date", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><Date>2030-01-01T00:00:00.000Z</Date></Expiration>")), nil},
		{"expiration date in other zone at midnight UTC", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><Date>2030-01-01T02:00:00+02:00</Date></Expiration>")), nil},
		{"expired object delete marker", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>")), nil},
		{"noncurrent", lifecycleConfig(lifecycleRule(filterAll +
			"<NoncurrentVersionExpiration><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration>")), nil},
		{"abort", lifecycleConfig(lifecycleRule(
			"<Filter><Prefix>x</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>")), nil},
		{"filter tag", lifecycleConfig(lifecycleRule(
			"<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>" + expire)), nil},
		{"filter and", lifecycleConfig(lifecycleRule(
			"<Filter><And><Prefix>p</Prefix><Tag><Key>k</Key><Value>v</Value></Tag><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan><ObjectSizeLessThan>10</ObjectSizeLessThan></And></Filter>" + expire)), nil},
		{"unique ids", lifecycleConfig(
			lifecycleRule("<ID>a</ID>"+filterAll+expire), lifecycleRule("<ID>b</ID>"+filterAll+expire)), nil},
		{"max rules", lifecycleConfig(manyRules[:maxLifecycleRules]...), nil},

		{"no rules", lifecycleConfig(), s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"too many rules", lifecycleConfig(manyRules...),
			s3err.GetLifecycleInvalidRequestErr("Lifecycle configuration cannot have more than 1000 rules")},
		{"no action", lifecycleConfig(lifecycleRule(filterAll)), s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"invalid status", lifecycleConfig(`<Rule><Status>On</Status>` + filterAll + expire + `</Rule>`),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"no filter or prefix", lifecycleConfig(lifecycleRule(expire)), s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"filter and prefix", lifecycleConfig(lifecycleRule(filterAll + "<Prefix></Prefix>" + expire)),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"duplicate id", lifecycleConfig(
			lifecycleRule("<ID>a</ID>"+filterAll+expire), lifecycleRule("<ID>a</ID>"+filterAll+expire)),
			s3err.GetInvalidArgLifecycleErr("ID", "a", "Rule ID must be unique. Found same ID for more than one rule")},
		{"id too long", lifecycleConfig(lifecycleRule("<ID>" + strings.Repeat("a", 256) + "</ID>" + filterAll + expire)),
			s3err.GetInvalidArgLifecycleErr("ID", strings.Repeat("a", 256), "ID length should not exceed allowed limit of 255")},
		{"id at limit", lifecycleConfig(lifecycleRule("<ID>" + strings.Repeat("a", 255) + "</ID>" + filterAll + expire)), nil},
		{"days zero", lifecycleConfig(lifecycleRule(filterAll + "<Expiration><Days>0</Days></Expiration>")),
			s3err.GetInvalidArgLifecycleErr("Days", "0", "'Days' for Expiration action must be a positive integer")},
		{"empty expiration", lifecycleConfig(lifecycleRule(filterAll + "<Expiration></Expiration>")),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"days and date", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><Days>1</Days><Date>2030-01-01T00:00:00Z</Date></Expiration>")),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"days and marker", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><Days>1</Days><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>")),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"date not at midnight", lifecycleConfig(lifecycleRule(filterAll +
			"<Expiration><Date>2030-01-01T10:00:00Z</Date></Expiration>")),
			s3err.GetInvalidArgLifecycleErr("Date", "2030-01-01T10:00:00Z", "'Date' must be at midnight GMT")},
		{"marker with tag", lifecycleConfig(lifecycleRule(
			"<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>")),
			s3err.GetLifecycleInvalidRequestErr("Tag-based filters cannot be specified with ExpiredObjectDeleteMarker")},
		{"noncurrent without days", lifecycleConfig(lifecycleRule(filterAll +
			"<NoncurrentVersionExpiration><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>")),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"noncurrent days zero", lifecycleConfig(lifecycleRule(filterAll +
			"<NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration>")),
			s3err.GetInvalidArgLifecycleErr("NoncurrentDays", "0", "'NoncurrentDays' in the NoncurrentVersionExpiration action must be a positive integer")},
		{"newer noncurrent too large", lifecycleConfig(lifecycleRule(filterAll +
			"<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>101</NewerNoncurrentVersions></NoncurrentVersionExpiration>")),
			s3err.GetInvalidArgLifecycleErr("NewerNoncurrentVersions", "101", "'NewerNoncurrentVersions' must be between 1 and 100")},
		{"abort without days", lifecycleConfig(lifecycleRule(filterAll +
			"<AbortIncompleteMultipartUpload></AbortIncompleteMultipartUpload>")),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"abort days zero", lifecycleConfig(lifecycleRule(filterAll +
			"<AbortIncompleteMultipartUpload><DaysAfterInitiation>0</DaysAfterInitiation></AbortIncompleteMultipartUpload>")),
			s3err.GetInvalidArgLifecycleErr("DaysAfterInitiation", "0", "'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer")},
		{"abort with tag", lifecycleConfig(lifecycleRule(
			"<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>")),
			s3err.GetLifecycleInvalidRequestErr("AbortIncompleteMultipartUpload cannot be specified with tag or object size filters")},
		{"abort with size", lifecycleConfig(lifecycleRule(
			"<Filter><ObjectSizeGreaterThan>5</ObjectSizeGreaterThan></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>")),
			s3err.GetLifecycleInvalidRequestErr("AbortIncompleteMultipartUpload cannot be specified with tag or object size filters")},
		{"abort with size in and", lifecycleConfig(lifecycleRule(
			"<Filter><And><Prefix>a</Prefix><ObjectSizeLessThan>5</ObjectSizeLessThan></And></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>")),
			s3err.GetLifecycleInvalidRequestErr("AbortIncompleteMultipartUpload cannot be specified with tag or object size filters")},
		{"abort with prefix", lifecycleConfig(lifecycleRule(
			"<Filter><Prefix>a</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>")), nil},
		{"newer noncurrent with legacy prefix", lifecycleConfig(lifecycleRule("<Prefix>a/</Prefix>" +
			"<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>")),
			s3err.GetLifecycleInvalidRequestErr("NewerNoncurrentVersions cannot be specified with a rule level Prefix, use a Filter")},
		{"noncurrent days with legacy prefix", lifecycleConfig(lifecycleRule("<Prefix>a/</Prefix>" +
			"<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>")), nil},
		{"filter with prefix and tag", lifecycleConfig(lifecycleRule(
			"<Filter><Prefix>a</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></Filter>" + expire)),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"empty and", lifecycleConfig(lifecycleRule("<Filter><And></And></Filter>" + expire)),
			s3err.GetAPIError(s3err.ErrMalformedXML)},
		{"empty tag key", lifecycleConfig(lifecycleRule(
			"<Filter><Tag><Key></Key><Value>v</Value></Tag></Filter>" + expire)),
			s3err.GetInvalidTagErr(s3err.ErrInvalidTagKey, "", "")},
		{"duplicate and tags", lifecycleConfig(lifecycleRule(
			"<Filter><And><Tag><Key>k</Key><Value>1</Value></Tag><Tag><Key>k</Key><Value>2</Value></Tag></And></Filter>" + expire)),
			s3err.GetInvalidTagErr(s3err.ErrDuplicateTagKey, "k", "")},
		{"negative size", lifecycleConfig(lifecycleRule(
			"<Filter><ObjectSizeGreaterThan>-1</ObjectSizeGreaterThan></Filter>" + expire)),
			s3err.GetInvalidArgLifecycleErr("ObjectSizeGreaterThan", "-1", "ObjectSizeGreaterThan must not be negative")},
		{"size bounds crossed", lifecycleConfig(lifecycleRule(
			"<Filter><And><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan><ObjectSizeLessThan>10</ObjectSizeLessThan></And></Filter>" + expire)),
			s3err.GetInvalidArgLifecycleErr("ObjectSizeLessThan", "10", "The ObjectSizeLessThan value must be greater than the ObjectSizeGreaterThan value")},
		{"transition", lifecycleConfig(lifecycleRule(filterAll +
			"<Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>")),
			s3err.GetAPIError(s3err.ErrNotImplemented)},
		{"noncurrent transition", lifecycleConfig(lifecycleRule(filterAll +
			"<NoncurrentVersionTransition><NoncurrentDays>1</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition>")),
			s3err.GetAPIError(s3err.ErrNotImplemented)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg LifecycleConfiguration
			assert.NoError(t, xml.Unmarshal([]byte(tt.xml), &cfg))
			err := cfg.Validate()
			if tt.err == nil {
				assert.NoError(t, err)
				return
			}
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestLifecycleRule_Criteria(t *testing.T) {
	parse := func(inner string) LifecycleRule {
		var cfg LifecycleConfiguration
		assert.NoError(t, xml.Unmarshal([]byte(lifecycleConfig(lifecycleRule(inner))), &cfg))
		return cfg.Rules[0]
	}

	rule := parse("<Prefix>legacy/</Prefix>")
	assert.Equal(t, LifecycleCriteria{Prefix: "legacy/"}, rule.Criteria())

	rule = parse("<Filter><Prefix>p/</Prefix></Filter>")
	assert.Equal(t, LifecycleCriteria{Prefix: "p/"}, rule.Criteria())

	rule = parse("<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>")
	assert.Equal(t, []Tag{{Key: "k", Value: "v"}}, rule.Criteria().Tags)

	rule = parse("<Filter><And><Prefix>p</Prefix><Tag><Key>k</Key><Value>v</Value></Tag><ObjectSizeGreaterThan>5</ObjectSizeGreaterThan></And></Filter>")
	c := rule.Criteria()
	assert.Equal(t, "p", c.Prefix)
	assert.Len(t, c.Tags, 1)
	assert.Equal(t, int64(5), *c.MinSize)
	assert.Nil(t, c.MaxSize)

	rule = parse("<Filter></Filter>")
	assert.Equal(t, LifecycleCriteria{}, rule.Criteria())
}

func TestParseLifecycleConfiguration_UnknownElements(t *testing.T) {
	const expire = "<Expiration><Days>1</Days></Expiration>"

	valid := lifecycleConfig(lifecycleRule(
		"<ID>a</ID><Filter><Prefix>p</Prefix></Filter>" + expire +
			"<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>" +
			"<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>"))
	cfg, err := ParseLifecycleConfiguration([]byte(valid))
	assert.NoError(t, err)
	assert.NoError(t, cfg.Validate())

	tests := []struct{ name, xml string }{
		{"configuration", `<LifecycleConfiguration><Bogus/><Rule><Status>Enabled</Status>` + expire + `</Rule></LifecycleConfiguration>`},
		{"rule", lifecycleConfig(lifecycleRule("<Bogus>1</Bogus>" + expire))},
		{"filter typo", lifecycleConfig(lifecycleRule("<Filter><prefix>tmp/</prefix></Filter>" + expire))},
		{"filter", lifecycleConfig(lifecycleRule("<Filter><Bogus/></Filter>" + expire))},
		{"and", lifecycleConfig(lifecycleRule("<Filter><And><Prefix>a</Prefix><Bogus/></And></Filter>" + expire))},
		{"tag", lifecycleConfig(lifecycleRule("<Filter><Tag><Key>k</Key><Value>v</Value><Bogus/></Tag></Filter>" + expire))},
		{"tag in and", lifecycleConfig(lifecycleRule("<Filter><And><Tag><Key>k</Key><Bogus/></Tag></And></Filter>" + expire))},
		{"expiration", lifecycleConfig(lifecycleRule("<Filter></Filter><Expiration><Days>1</Days><Bogus/></Expiration>"))},
		{"noncurrent expiration", lifecycleConfig(lifecycleRule("<Filter></Filter>" +
			"<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><Bogus/></NoncurrentVersionExpiration>"))},
		{"abort", lifecycleConfig(lifecycleRule("<Filter></Filter>" +
			"<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation><Bogus/></AbortIncompleteMultipartUpload>"))},
		{"not xml", "<LifecycleConfiguration><Rule>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseLifecycleConfiguration([]byte(tt.xml))
			assert.Equal(t, s3err.GetAPIError(s3err.ErrMalformedXML), err)
		})
	}
}

func TestLifecycleConfiguration_HugeDays(t *testing.T) {
	cfg, err := ParseLifecycleConfiguration([]byte(lifecycleConfig(lifecycleRule(
		"<Filter></Filter><NoncurrentVersionExpiration><NoncurrentDays>2147483647</NoncurrentDays></NoncurrentVersionExpiration>"))))
	assert.NoError(t, err)
	assert.NoError(t, cfg.Validate())
}
