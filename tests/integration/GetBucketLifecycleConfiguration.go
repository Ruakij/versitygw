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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
)

func GetBucketLifecycleConfiguration_non_existing_bucket(s *S3Conf) error {
	testName := "GetBucketLifecycleConfiguration_non_existing_bucket"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		_, err := s3client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: getPtr("non-existing-bucket"),
		})
		cancel()
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrNoSuchBucket))
	})
}

func GetBucketLifecycleConfiguration_no_such_lifecycle_configuration(s *S3Conf) error {
	testName := "GetBucketLifecycleConfiguration_no_such_lifecycle_configuration"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		_, err := s3client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: &bucket,
		})
		cancel()
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrNoSuchLifecycleConfiguration))
	})
}

func GetBucketLifecycleConfiguration_success(s *S3Conf) error {
	testName := "GetBucketLifecycleConfiguration_success"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: &bucket,
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{
					lifecycleExpireRule("expire", 30),
					{
						ID:     getPtr("noncurrent"),
						Status: types.ExpirationStatusDisabled,
						Filter: &types.LifecycleRuleFilter{Prefix: getPtr("kopia/")},
						NoncurrentVersionExpiration: &types.NoncurrentVersionExpiration{
							NoncurrentDays:          getPtr(int32(7)),
							NewerNoncurrentVersions: getPtr(int32(3)),
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

		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		res, err := s3client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: &bucket,
		})
		cancel()
		if err != nil {
			return err
		}

		if len(res.Rules) != 2 {
			return fmt.Errorf("expected 2 rules, instead got %v", len(res.Rules))
		}

		expire, noncurrent := res.Rules[0], res.Rules[1]
		if getString(expire.ID) != "expire" || expire.Status != types.ExpirationStatusEnabled {
			return fmt.Errorf("unexpected first rule: %+v", expire)
		}
		if expire.Filter == nil || getString(expire.Filter.Prefix) != "logs/" {
			return fmt.Errorf("unexpected first rule filter: %+v", expire.Filter)
		}
		if expire.Expiration == nil || expire.Expiration.Days == nil || *expire.Expiration.Days != 30 {
			return fmt.Errorf("unexpected first rule expiration: %+v", expire.Expiration)
		}

		if getString(noncurrent.ID) != "noncurrent" || noncurrent.Status != types.ExpirationStatusDisabled {
			return fmt.Errorf("unexpected second rule: %+v", noncurrent)
		}
		nve := noncurrent.NoncurrentVersionExpiration
		if nve == nil || nve.NoncurrentDays == nil || *nve.NoncurrentDays != 7 ||
			nve.NewerNoncurrentVersions == nil || *nve.NewerNoncurrentVersions != 3 {
			return fmt.Errorf("unexpected noncurrent version expiration: %+v", nve)
		}
		abort := noncurrent.AbortIncompleteMultipartUpload
		if abort == nil || abort.DaysAfterInitiation == nil || *abort.DaysAfterInitiation != 2 {
			return fmt.Errorf("unexpected abort incomplete multipart upload: %+v", abort)
		}

		return nil
	})
}
