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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
)

func DeleteBucketLifecycle_non_existing_bucket(s *S3Conf) error {
	testName := "DeleteBucketLifecycle_non_existing_bucket"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		_, err := s3client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{
			Bucket: getPtr("non-existing-bucket"),
		})
		cancel()
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrNoSuchBucket))
	})
}

func DeleteBucketLifecycle_success(s *S3Conf) error {
	testName := "DeleteBucketLifecycle_success"
	return actionHandler(s, testName, func(s3client *s3.Client, bucket string) error {
		deleteLifecycle := func() error {
			ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
			_, err := s3client.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{
				Bucket: &bucket,
			})
			cancel()
			return err
		}

		// deleting an unset lifecycle configuration is not an error
		if err := deleteLifecycle(); err != nil {
			return err
		}

		err := putBucketLifecycle(s3client, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: &bucket,
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{lifecycleExpireRule("rule", 1)},
			},
		})
		if err != nil {
			return err
		}

		if err := deleteLifecycle(); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		_, err = s3client.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
			Bucket: &bucket,
		})
		cancel()
		return checkApiErr(err, s3err.GetAPIError(s3err.ErrNoSuchLifecycleConfiguration))
	})
}
