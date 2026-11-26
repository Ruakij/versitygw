// Copyright 2023 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
)

func putBucketLifecycle(client *s3.Client, input *s3.PutBucketLifecycleConfigurationInput) error {
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	_, err := client.PutBucketLifecycleConfiguration(ctx, input)
	cancel()
	return err
}

func lifecycleExpireRule(id string, days int32) types.LifecycleRule {
	return types.LifecycleRule{
		ID:         &id,
		Status:     types.ExpirationStatusEnabled,
		Filter:     &types.LifecycleRuleFilter{Prefix: getPtr("logs/")},
		Expiration: &types.LifecycleExpiration{Days: &days},
	}
}

func PutBucketLifecycleConfiguration_non_existing_bucket(s *S3Conf) error {
	testName := "PutBucketLifecycleConfiguration_non_existing_bucket"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: getPtr("non-existing-bucket"),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{lifecycleExpireRule("rule", 1)},
			},
		})
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrNoSuchBucket))
	})
}

func PutBucketLifecycleConfiguration_empty_rules(s *S3Conf) error {
	testName := "PutBucketLifecycleConfiguration_empty_rules"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 &bucket,
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{}},
		})
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrMalformedXML))
	})
}

func PutBucketLifecycleConfiguration_invalid_rules(s *S3Conf) error {
	testName := "PutBucketLifecycleConfiguration_invalid_rules"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		days := func(d int32) *types.LifecycleExpiration { return &types.LifecycleExpiration{Days: &d} }
		filter := &types.LifecycleRuleFilter{Prefix: getPtr("")}
		longID := strings.Repeat("a", 256)

		for i, test := range []struct {
			rules       []types.LifecycleRule
			expectedErr s3err.S3Error
		}{
			{ // invalid status
				[]types.LifecycleRule{{Status: "On", Filter: filter, Expiration: days(1)}},
				s3err.GetAPIError(s3err.ErrMalformedXML),
			},
			{ // no action
				[]types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: filter}},
				s3err.GetAPIError(s3err.ErrMalformedXML),
			},
			{ // no filter or prefix
				[]types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Expiration: days(1)}},
				s3err.GetAPIError(s3err.ErrMalformedXML),
			},
			{ // duplicate rule id
				[]types.LifecycleRule{lifecycleExpireRule("same", 1), lifecycleExpireRule("same", 2)},
				s3err.GetInvalidArgLifecycleErr("ID", "same", "Rule ID must be unique. Found same ID for more than one rule"),
			},
			{ // rule id too long
				[]types.LifecycleRule{lifecycleExpireRule(longID, 1)},
				s3err.GetInvalidArgLifecycleErr("ID", longID, "ID length should not exceed allowed limit of 255"),
			},
			{ // zero days
				[]types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: filter, Expiration: days(0)}},
				s3err.GetInvalidArgLifecycleErr("Days", "0", "'Days' for Expiration action must be a positive integer"),
			},
			{ // days together with the expired object delete marker
				[]types.LifecycleRule{{
					Status: types.ExpirationStatusEnabled,
					Filter: filter,
					Expiration: &types.LifecycleExpiration{
						Days:                      getPtr(int32(1)),
						ExpiredObjectDeleteMarker: getPtr(true),
					},
				}},
				s3err.GetAPIError(s3err.ErrMalformedXML),
			},
			{ // noncurrent expiration without days
				[]types.LifecycleRule{{
					Status:                      types.ExpirationStatusEnabled,
					Filter:                      filter,
					NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{NewerNoncurrentVersions: getPtr(int32(1))},
				}},
				s3err.GetAPIError(s3err.ErrMalformedXML),
			},
			{ // abort incomplete multipart upload with a tag filter
				[]types.LifecycleRule{{
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Tag: &types.Tag{Key: getPtr("k"), Value: getPtr("v")}},
					AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: getPtr(int32(1)),
					},
				}},
				s3err.GetLifecycleInvalidRequestErr("AbortIncompleteMultipartUpload cannot be specified with tag or object size filters"),
			},
			{ // transitions are not supported
				[]types.LifecycleRule{{
					Status: types.ExpirationStatusEnabled,
					Filter: filter,
					Transitions: []types.Transition{{
						Days:         getPtr(int32(30)),
						StorageClass: types.TransitionStorageClassGlacier,
					}},
				}},
				s3err.GetAPIError(s3err.ErrNotImplemented),
			},
		} {
			err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
				Bucket:                 &bucket,
				LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: test.rules},
			})
			if err := checkApiErr(err, test.expectedErr); err != nil {
				return fmt.Errorf("test %v failed: %w", i+1, err)
			}
		}

		return nil
	})
}

func PutBucketLifecycleConfiguration_success(s *S3Conf) error {
	testName := "PutBucketLifecycleConfiguration_success"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: &bucket,
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{
					lifecycleExpireRule("expire", 30),
					{
						ID:     getPtr("noncurrent"),
						Status: types.ExpirationStatusEnabled,
						Filter: &types.LifecycleRuleFilter{And: &types.LifecycleRuleAndOperator{
							Prefix:                getPtr("kopia/"),
							Tags:                  []types.Tag{{Key: getPtr("k"), Value: getPtr("v")}},
							ObjectSizeGreaterThan: getPtr(int64(10)),
						}},
						NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
							NoncurrentDays:          getPtr(int32(7)),
							NewerNoncurrentVersions: getPtr(int32(3)),
						},
					},
					{
						ID:     getPtr("cleanup"),
						Status: types.ExpirationStatusDisabled,
						Filter: &types.LifecycleRuleFilter{Prefix: getPtr("")},
						Expiration: &types.LifecycleExpiration{
							ExpiredObjectDeleteMarker: getPtr(true),
						},
						AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
							DaysAfterInitiation: getPtr(int32(2)),
						},
					},
				},
			},
		})
		if err != nil {
			return err
		}

		// a second put replaces the configuration
		return putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: &bucket,
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{lifecycleExpireRule("expire", 5)},
			},
		})
	})
}
