# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
import logging
from itertools import imap

import boto3


logger = logging.getLogger(__name__)


class ObjectSummary:
    """A serializable version of boto3.resources.factory.s3.ObjectSummary"""
    def __init__(self, key, size):
        self.key = key
        self.size = size


class S3Store:

    def __init__(self, bucket_name):
        # self.bucket = boto3.resource('s3').Bucket(bucket_name)
        self.bucket_name = bucket_name

    def list_keys(self, prefix):
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        keys = bucket.objects.filter(Prefix=prefix)
        return imap(lambda x: ObjectSummary(x.key, x.size), keys)

    def list_folders(self, prefix='', delimiter='/'):
        paginator = boto3.client('s3').get_paginator('list_objects')
        result = paginator.paginate(Bucket=self.bucket_name,
                                    Prefix=prefix,
                                    Delimiter=delimiter)
        for page in result:
            common_prefixes = page.get('CommonPrefixes')
            if common_prefixes:
                for item in common_prefixes:
                    yield item['Prefix']

    def get_key(self, key):
        bucket = bucket = boto3.resource('s3').Bucket(self.bucket_name)
        try:
            return bucket.Object(key).get()['Body'].read()
        except:
            raise Exception('Error retrieving key "{}" from S3'.format(key))

    def upload_file(self, file_obj, prefix, name):
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        key = ''.join([prefix, name])
        logger.info('Uploading file to {}:{}'.format(self.bucket_name, key))
        bucket.put_object(Key=key, Body=file_obj)

    def delete_key(self, key):
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        bucket.Object(key).delete()

    def is_prefix_empty(self, prefix):
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        result = bucket.objects.filter(Prefix=prefix).limit(1)
        return len(list(result)) == 0
