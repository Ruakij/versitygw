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
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf16"

	"github.com/versity/versitygw/debuglogger"
	"github.com/versity/versitygw/s3err"
)

const (
	maxLifecycleRules              = 1000
	maxLifecycleRuleIDLength       = 255
	maxNewerNoncurrentVersions     = 100
	LifecycleStatusEnabled         = "Enabled"
	LifecycleStatusDisabled        = "Disabled"
	maxLifecycleTagKeyLength       = 128
	maxLifecycleTagValueLength     = 256
	lifecycleExpirationDateMessage = "'Date' must be at midnight GMT"
)

// LifecycleConfiguration represents the S3 bucket lifecycle configuration.
type LifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []LifecycleRule `xml:"Rule"`
}

// LifecycleRule is a single rule of a lifecycle configuration.
type LifecycleRule struct {
	ID     *string `xml:"ID,omitempty"`
	Status string  `xml:"Status"`
	// Filter selects the objects of the rule. The legacy Prefix element at
	// the rule level is the alternative to it.
	Filter                         *LifecycleFilter                `xml:"Filter,omitempty"`
	Prefix                         *string                         `xml:"Prefix,omitempty"`
	Expiration                     *LifecycleExpiration            `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	Transitions                    []LifecycleTransition           `xml:"Transition,omitempty"`
	NoncurrentVersionTransitions   []NoncurrentVersionTransition   `xml:"NoncurrentVersionTransition,omitempty"`
}

// LifecycleFilter selects the objects a rule applies to. At most one of its
// elements may be set, And combines several predicates.
type LifecycleFilter struct {
	Prefix                *string             `xml:"Prefix,omitempty"`
	Tag                   *Tag                `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan *int64              `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64              `xml:"ObjectSizeLessThan,omitempty"`
	And                   *LifecycleFilterAnd `xml:"And,omitempty"`
}

// LifecycleFilterAnd combines the predicates of a filter, all of which have
// to match.
type LifecycleFilterAnd struct {
	Prefix                *string `xml:"Prefix,omitempty"`
	Tags                  []Tag   `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan *int64  `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64  `xml:"ObjectSizeLessThan,omitempty"`
}

// LifecycleExpiration expires the current version of an object. Exactly one
// of its elements is set.
type LifecycleExpiration struct {
	Days                      *int32     `xml:"Days,omitempty"`
	Date                      *time.Time `xml:"Date,omitempty"`
	ExpiredObjectDeleteMarker *bool      `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

// NoncurrentVersionExpiration permanently deletes noncurrent versions.
type NoncurrentVersionExpiration struct {
	NoncurrentDays          *int32 `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions *int32 `xml:"NewerNoncurrentVersions,omitempty"`
}

// AbortIncompleteMultipartUpload aborts multipart uploads that are not
// completed within the number of days after their initiation.
type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int32 `xml:"DaysAfterInitiation,omitempty"`
}

// LifecycleTransition is parsed to be rejected: storage classes are not
// supported.
type LifecycleTransition struct {
	Days         *int32     `xml:"Days,omitempty"`
	Date         *time.Time `xml:"Date,omitempty"`
	StorageClass string     `xml:"StorageClass,omitempty"`
}

// NoncurrentVersionTransition is parsed to be rejected: storage classes are
// not supported.
type NoncurrentVersionTransition struct {
	NoncurrentDays          *int32 `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions *int32 `xml:"NewerNoncurrentVersions,omitempty"`
	StorageClass            string `xml:"StorageClass,omitempty"`
}

// LifecycleCriteria are the predicates a rule selects objects with, all of
// which have to match.
type LifecycleCriteria struct {
	Prefix string
	Tags   []Tag
	// MinSize and MaxSize are exclusive bounds on the object size
	MinSize *int64
	MaxSize *int64
}

// Criteria returns the object selection of the rule, whichever of the
// legacy prefix, the filter prefix, the filter tag, the size bounds or And
// it is written with.
func (r *LifecycleRule) Criteria() LifecycleCriteria {
	var c LifecycleCriteria
	if r.Prefix != nil {
		c.Prefix = *r.Prefix
	}
	f := r.Filter
	if f == nil {
		return c
	}
	if f.Prefix != nil {
		c.Prefix = *f.Prefix
	}
	if f.Tag != nil {
		c.Tags = []Tag{*f.Tag}
	}
	c.MinSize = f.ObjectSizeGreaterThan
	c.MaxSize = f.ObjectSizeLessThan
	if f.And != nil {
		if f.And.Prefix != nil {
			c.Prefix = *f.And.Prefix
		}
		c.Tags = f.And.Tags
		c.MinSize = f.And.ObjectSizeGreaterThan
		c.MaxSize = f.And.ObjectSizeLessThan
	}
	return c
}

// Enabled reports whether the rule is in effect.
func (r *LifecycleRule) Enabled() bool {
	return r.Status == LifecycleStatusEnabled
}

// lifecycleElements are the elements each element of a lifecycle
// configuration may contain.
var lifecycleElements = map[string][]string{
	"LifecycleConfiguration": {"Rule"},
	"Rule": {"ID", "Status", "Filter", "Prefix", "Expiration", "NoncurrentVersionExpiration",
		"AbortIncompleteMultipartUpload", "Transition", "NoncurrentVersionTransition"},
	"Filter":                         {"Prefix", "Tag", "ObjectSizeGreaterThan", "ObjectSizeLessThan", "And"},
	"And":                            {"Prefix", "Tag", "ObjectSizeGreaterThan", "ObjectSizeLessThan"},
	"Expiration":                     {"Days", "Date", "ExpiredObjectDeleteMarker"},
	"NoncurrentVersionExpiration":    {"NoncurrentDays", "NewerNoncurrentVersions"},
	"AbortIncompleteMultipartUpload": {"DaysAfterInitiation"},
	"Tag":                            {"Key", "Value"},
}

// checkLifecycleElements fails for an element S3 does not know in the places
// that are parsed. Decoding skips such elements, and a misspelled element of
// a filter would turn a rule into one that selects every object.
func checkLifecycleElements(data []byte) error {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var path []string
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if len(path) > 0 {
				if allowed, ok := lifecycleElements[path[len(path)-1]]; ok {
					found := false
					for _, name := range allowed {
						if name == t.Name.Local {
							found = true
							break
						}
					}
					if !found {
						return fmt.Errorf("unknown element %q in %q", t.Name.Local, path[len(path)-1])
					}
				}
			}
			path = append(path, t.Name.Local)
		case xml.EndElement:
			path = path[:len(path)-1]
		}
	}
}

// ParseLifecycleConfiguration parses the lifecycle configuration of a
// request, which has to consist of known elements only.
func ParseLifecycleConfiguration(data []byte) (*LifecycleConfiguration, error) {
	if err := checkLifecycleElements(data); err != nil {
		debuglogger.Logf("invalid lifecycle config: %v", err)
		return nil, s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	var config LifecycleConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		debuglogger.Logf("unmarshal lifecycle config: %v", err)
		return nil, s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	return &config, nil
}

// ParseLifecycleConfigOutput parses a stored lifecycle configuration.
func ParseLifecycleConfigOutput(data []byte) (*LifecycleConfiguration, error) {
	var config LifecycleConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		debuglogger.Logf("unmarshal lifecycle config: %v", err)
		return nil, fmt.Errorf("failed to parse lifecycle config: %w", err)
	}
	return &config, nil
}

// Validate checks the lifecycle configuration the way S3 does.
func (c *LifecycleConfiguration) Validate() error {
	if len(c.Rules) == 0 {
		debuglogger.Logf("lifecycle configuration without rules")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if len(c.Rules) > maxLifecycleRules {
		debuglogger.Logf("too many lifecycle rules: %d", len(c.Rules))
		return s3err.GetLifecycleInvalidRequestErr(
			fmt.Sprintf("Lifecycle configuration cannot have more than %d rules", maxLifecycleRules))
	}

	ids := make(map[string]struct{}, len(c.Rules))
	for i := range c.Rules {
		rule := &c.Rules[i]
		if rule.ID != nil {
			if len(*rule.ID) > maxLifecycleRuleIDLength {
				debuglogger.Logf("lifecycle rule ID too long: %d", len(*rule.ID))
				return s3err.GetInvalidArgLifecycleErr("ID", *rule.ID,
					fmt.Sprintf("ID length should not exceed allowed limit of %d", maxLifecycleRuleIDLength))
			}
			if _, ok := ids[*rule.ID]; ok {
				debuglogger.Logf("duplicate lifecycle rule ID: %s", *rule.ID)
				return s3err.GetInvalidArgLifecycleErr("ID", *rule.ID,
					"Rule ID must be unique. Found same ID for more than one rule")
			}
			ids[*rule.ID] = struct{}{}
		}
		if err := rule.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks a single lifecycle rule.
func (r *LifecycleRule) Validate() error {
	if r.Status != LifecycleStatusEnabled && r.Status != LifecycleStatusDisabled {
		debuglogger.Logf("invalid lifecycle rule status: %q", r.Status)
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	if (r.Filter == nil) == (r.Prefix == nil) {
		debuglogger.Logf("lifecycle rule needs exactly one of Filter and Prefix")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if err := r.Filter.validate(); err != nil {
		return err
	}

	hasExpiration := r.Expiration != nil
	hasNoncurrent := r.NoncurrentVersionExpiration != nil
	hasAbort := r.AbortIncompleteMultipartUpload != nil
	hasTransition := len(r.Transitions) > 0 || len(r.NoncurrentVersionTransitions) > 0
	if !hasExpiration && !hasNoncurrent && !hasAbort && !hasTransition {
		debuglogger.Logf("lifecycle rule without action")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	hasTags := len(r.Criteria().Tags) > 0
	if hasExpiration {
		if err := r.Expiration.validate(hasTags); err != nil {
			return err
		}
	}
	if hasNoncurrent {
		if err := r.NoncurrentVersionExpiration.validate(r.Prefix != nil); err != nil {
			return err
		}
	}
	if hasAbort {
		c := r.Criteria()
		if err := r.AbortIncompleteMultipartUpload.validate(hasTags || c.MinSize != nil || c.MaxSize != nil); err != nil {
			return err
		}
	}
	if hasTransition {
		debuglogger.Logf("lifecycle transitions are not supported")
		return s3err.GetAPIError(s3err.ErrNotImplemented)
	}

	return nil
}

func (f *LifecycleFilter) validate() error {
	if f == nil {
		return nil
	}

	set := 0
	for _, present := range []bool{f.Prefix != nil, f.Tag != nil,
		f.ObjectSizeGreaterThan != nil, f.ObjectSizeLessThan != nil, f.And != nil} {
		if present {
			set++
		}
	}
	if set > 1 {
		debuglogger.Logf("lifecycle filter with more than one element, use And")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	if f.Tag != nil {
		if err := validateLifecycleTags([]Tag{*f.Tag}); err != nil {
			return err
		}
	}
	if err := validateLifecycleSizes(f.ObjectSizeGreaterThan, f.ObjectSizeLessThan); err != nil {
		return err
	}

	if a := f.And; a != nil {
		if a.Prefix == nil && len(a.Tags) == 0 && a.ObjectSizeGreaterThan == nil && a.ObjectSizeLessThan == nil {
			debuglogger.Logf("empty lifecycle filter And")
			return s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if err := validateLifecycleTags(a.Tags); err != nil {
			return err
		}
		if err := validateLifecycleSizes(a.ObjectSizeGreaterThan, a.ObjectSizeLessThan); err != nil {
			return err
		}
	}

	return nil
}

func validateLifecycleTags(tags []Tag) error {
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if l := utf16Length(tag.Key); l == 0 || l > maxLifecycleTagKeyLength {
			return s3err.GetInvalidTagErr(s3err.ErrInvalidTagKey, tag.Key, "")
		}
		if utf16Length(tag.Value) > maxLifecycleTagValueLength {
			return s3err.GetInvalidTagErr(s3err.ErrInvalidTagValue, tag.Key, tag.Value)
		}
		if _, ok := seen[tag.Key]; ok {
			return s3err.GetInvalidTagErr(s3err.ErrDuplicateTagKey, tag.Key, "")
		}
		seen[tag.Key] = struct{}{}
	}
	return nil
}

func utf16Length(s string) int {
	var n int
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

func validateLifecycleSizes(greater, less *int64) error {
	if greater != nil && *greater < 0 {
		return s3err.GetInvalidArgLifecycleErr("ObjectSizeGreaterThan", fmt.Sprint(*greater),
			"ObjectSizeGreaterThan must not be negative")
	}
	if less != nil && *less < 0 {
		return s3err.GetInvalidArgLifecycleErr("ObjectSizeLessThan", fmt.Sprint(*less),
			"ObjectSizeLessThan must not be negative")
	}
	if greater != nil && less != nil && *less <= *greater {
		return s3err.GetInvalidArgLifecycleErr("ObjectSizeLessThan", fmt.Sprint(*less),
			"The ObjectSizeLessThan value must be greater than the ObjectSizeGreaterThan value")
	}
	return nil
}

func (e *LifecycleExpiration) validate(hasTags bool) error {
	markerSet := e.ExpiredObjectDeleteMarker != nil && *e.ExpiredObjectDeleteMarker
	set := 0
	for _, present := range []bool{e.Days != nil, e.Date != nil, markerSet} {
		if present {
			set++
		}
	}
	if set != 1 {
		debuglogger.Logf("lifecycle expiration needs exactly one of Days, Date and ExpiredObjectDeleteMarker")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	if e.Days != nil && *e.Days <= 0 {
		return s3err.GetInvalidArgLifecycleErr("Days", fmt.Sprint(*e.Days),
			"'Days' for Expiration action must be a positive integer")
	}
	if e.Date != nil {
		d := e.Date.UTC()
		if d.Hour() != 0 || d.Minute() != 0 || d.Second() != 0 || d.Nanosecond() != 0 {
			return s3err.GetInvalidArgLifecycleErr("Date", e.Date.Format(time.RFC3339), lifecycleExpirationDateMessage)
		}
	}
	if markerSet && hasTags {
		return s3err.GetLifecycleInvalidRequestErr(
			"Tag-based filters cannot be specified with ExpiredObjectDeleteMarker")
	}

	return nil
}

func (e *NoncurrentVersionExpiration) validate(legacyPrefix bool) error {
	if e.NoncurrentDays == nil {
		debuglogger.Logf("NoncurrentVersionExpiration without NoncurrentDays")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if *e.NoncurrentDays <= 0 {
		return s3err.GetInvalidArgLifecycleErr("NoncurrentDays", fmt.Sprint(*e.NoncurrentDays),
			"'NoncurrentDays' in the NoncurrentVersionExpiration action must be a positive integer")
	}
	if n := e.NewerNoncurrentVersions; n != nil && (*n < 1 || *n > maxNewerNoncurrentVersions) {
		return s3err.GetInvalidArgLifecycleErr("NewerNoncurrentVersions", fmt.Sprint(*n),
			fmt.Sprintf("'NewerNoncurrentVersions' must be between 1 and %d", maxNewerNoncurrentVersions))
	}
	if e.NewerNoncurrentVersions != nil && legacyPrefix {
		return s3err.GetLifecycleInvalidRequestErr(
			"NewerNoncurrentVersions cannot be specified with a rule level Prefix, use a Filter")
	}
	return nil
}

func (a *AbortIncompleteMultipartUpload) validate(hasFilters bool) error {
	if a.DaysAfterInitiation == nil {
		debuglogger.Logf("AbortIncompleteMultipartUpload without DaysAfterInitiation")
		return s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	if *a.DaysAfterInitiation <= 0 {
		return s3err.GetInvalidArgLifecycleErr("DaysAfterInitiation", fmt.Sprint(*a.DaysAfterInitiation),
			"'DaysAfterInitiation' for AbortIncompleteMultipartUpload action must be a positive integer")
	}
	if hasFilters {
		return s3err.GetLifecycleInvalidRequestErr(
			"AbortIncompleteMultipartUpload cannot be specified with tag or object size filters")
	}
	return nil
}
