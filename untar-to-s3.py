#!/usr/bin/env python
"""
Utility script for efficiently unpacking a tarball to an S3 bucket.

The script can load most tar files (e.g. .tar, .tar.gz) and uploads all files to an S3 bucket with an optional
prefix.

The script will automatically gzip certain file types, and will add a 'Cache-Control' header.

Requirements:
    Python 2.7+
    boto3 library (to install: sudo pip install boto3)

For usage overview::
    python untar-to-s3.py -h

Example::
    export AWS_ACCESS_KEY_ID=<iam account with PutObject permission>
    export AWS_SECRET_ACCESS_KEY=<secret from above account>
    python untar-to-s3.py web-assets-1.2.23.tar.gz --bucket my-bucket-name --prefix production

"""
from __future__ import print_function

import os
import sys
import mimetypes
import tarfile
import logging
import gzip
import argparse
import io
from multiprocessing.pool import ThreadPool

import boto3
s3 = boto3.resource('s3')

# Files of these types will be gzipped before being uploaded to S3 (unless disabled with --no-compress)
# This list comes from MaxCDN's Gzip compression settings.
COMPRESSIBLE_FILE_TYPES = ("text/plain",
                           "text/html",
                           "text/javascript",
                           "text/css",
                           "text/xml",
                           "application/javascript",
                           "application/x-javascript",
                           "application/xml",
                           "text/x-component",
                           "application/json",
                           "application/xhtml+xml",
                           "application/rss+xml",
                           "application/atom+xml",
                           "app/vdn.ms-fontobject",
                           "image/svg+xml",
                           "application/x-font-ttf",
                           "font/opentype",
                           "application/octet-stream",
)


def __deploy_asset_to_s3(data, path, size, bucket_name, compress=True):
    """
    Deploy a single asset file to an S3 bucket
    """

    try:
        kwargs = {
            'ContentType': mimetypes.guess_type(path)[0] or 'application/octet-stream',
            'CacheControl': "public, max-age=31536000",
            'ContentLength': size,
            'StorageClass': 'STANDARD',
            'ACL': 'public-read',
        }

        # gzip the file if appropriate
        if kwargs['ContentType'] in COMPRESSIBLE_FILE_TYPES and compress:
            new_buffer = io.BytesIO()
            gz_fd = gzip.GzipFile(compresslevel=9, mode="wb", fileobj=new_buffer)
            gz_fd.write(data)
            gz_fd.close()

            kwargs['ContentEncoding'] = 'gzip'
            kwargs['ContentLength'] = new_buffer.tell()

            new_buffer.seek(0)
            data = new_buffer.read()

        logging.debug("Uploading %s (%s bytes)" % (path, kwargs['ContentLength']))
        s3.Object(bucket_name, path).put(Body=data, **kwargs)

    except Exception as e:
        import traceback
        print(traceback.format_exc(e), file=sys.stderr)
        return 0

    # Return number of bytes uploaded.
    return kwargs['ContentLength']


def deploy_tarball_to_s3(tarball_obj, bucket_name, prefix='', region='us-west-2', concurrency=50, no_compress=False, strip_components=0):
    """
    Upload the contents of `tarball_obj`, a File-like object representing a valid .tar.gz file, to the S3 bucket `bucket_name`
    """
    # Connect to S3 and get a reference to the bucket name we will push files to
    conn = boto3.client('s3', region)
    if conn is None:
        logging.error("Invalid AWS region %s" % region)
        return

    # Ensure bucket exists before continuing
    from botocore.client import ClientError
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError:
        logging.error("S3 bucket %s does not exist in region %s" % (bucket_name, region))
        return

    # Open the tarball
    try:
        with tarfile.open(name=None, mode="r:*", fileobj=tarball_obj) as tarball:

            files_uploaded = 0

            # Parallelize the uploads so they don't take ages
            pool = ThreadPool(concurrency)

            # Iterate over the tarball's contents.
            try:
                for member in tarball:

                    # Ignore directories, links, devices, fifos, etc.
                    if not member.isfile():
                        continue

                    # Mimic the behaviour of tar -x --strip-components=
                    stripped_name = member.name.split('/')[strip_components:]
                    if not bool(stripped_name):
                        continue

                    path = os.path.join(prefix, '/'.join(stripped_name))

                    # Read file data from the tarball
                    fd = tarball.extractfile(member)

                    # Send a job to the pool.
                    pool.apply_async(__deploy_asset_to_s3, (fd.read(), path, member.size, bucket_name, not no_compress))

                    files_uploaded += 1

                # Wait for all transfers to finish
                pool.close()
                pool.join()  # Wait for all transfers to finish

            except KeyboardInterrupt:
                # Ctrl-C pressed
                print("Cancelling upload...")
                pool.close()
            finally:
                pool.close()
                pool.join()  # Wait for all transfers to finish
                print("Uploaded %i files" % (files_uploaded))

    except tarfile.ReadError:
        print("Unable to read asset tarfile", file=sys.stderr)
        return


def main():
    parser = argparse.ArgumentParser(description="Unpack a tarball from the local filesystem to an S3 bucket")
    parser.add_argument("-b", "--bucket", dest="bucket", type=str, required=True, help="Name of S3 bucket to unpack to")
    parser.add_argument("-p", "--prefix", dest="prefix", type=str, default='', help="Prefix this to the path of all uploaded files")
    parser.add_argument("-r", "--region", dest="region", type=str, default="us-west-2",
                        help="Region of S3 bucket. Default=us-west-2")
    parser.add_argument("-c", dest="concurrency", type=int, default=50,
                        help="Number of concurrent uploads. default=50")
    parser.add_argument("--debug", action="store_true", help="show verbose debug output")
    parser.add_argument("--no-compress", action="store_true",
                        help="disable gzip compression of known file types")
    parser.add_argument("--strip-components", dest="strip_components", type=int, default=0,
                        help="Remove the specified number of leading path elements. Pathnames with fewer elements will be silently skipped. Default=0")
    parser.add_argument("filename", type=str, help="File to load")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open(args.filename, "rb") as fd:
        deploy_tarball_to_s3(fd, args.bucket, args.prefix, args.region, args.concurrency, args.no_compress, args.strip_components)


if __name__ == "__main__":
    main()
