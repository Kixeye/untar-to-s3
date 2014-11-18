"""
Utility script for efficiently unpacking a tarball to an S3 bucket.

The script can load most tar files (e.g. .tar, .tar.gz) and uploads all files to an S3 bucket with an optional
prefix.

The script will automatically gzip certain file types, and will add a 'Cache-Control' header.

Requirements:
    Python 2.7
    boto library (to install: sudo pip install boto)

Recommended:
    gevent library to parallelize uploads to S3 (to install: sudo pip install gevent)

For usage overview::
    python untar-to-s3.py -h

Example::
    export AWS_ACCESS_KEY_ID=<iam account with PubObject permission>
    export AWS_SECRET_ACCESS_KEY=<secret from above account>
    python untar-to-s3.py web-assets-1.2.23.tar.gz --bucket my-bucket-name --prefix production

"""
from __future__ import print_function

# Gevent is an asynchronous IO library. It needs to be imported first.
try:
    import gevent
    from gevent import monkey; monkey.patch_all()
    from gevent.pool import Pool
except ImportError:
    class Pool:
        """Stub implementation of gevent.pool.Pool"""
        def __init__(self, *args, **kwargs):
            pass
        def wait_available(self):
            pass
        def join(self):
            pass
        def apply_async(self, callable, args, **kwargs):
            callable(*args, **kwargs)

import os
import sys
import mimetypes
import tarfile
import logging
import gzip
import signal
import argparse
import io

# TODO: Use boto3 once it is ready for production
import boto
from boto.s3 import connect_to_region
from boto.s3.key import Key

# Disable boto's debug logging. It's not helpful.
logger1 = logging.getLogger('boto')
logger1.setLevel("INFO")

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


def __deploy_asset_to_s3(data, path, size, bucket, compress=True):
    """
    Deploy a single asset file to an S3 bucket
    """

    try:
        headers = {
            'Content-Type': mimetypes.guess_type(path)[0],
            'Cache-Control': "public, max-age=31536000",
            'Content-Length': size,
        }

        # gzip the file if appropriate
        if mimetypes.guess_type(path)[0] in COMPRESSIBLE_FILE_TYPES and compress:
            new_buffer = io.BytesIO()
            gz_fd = gzip.GzipFile(compresslevel=9, mode="wb", fileobj=new_buffer)
            gz_fd.write(data)
            gz_fd.close()

            headers['Content-Encoding'] = 'gzip'
            headers['Content-Length'] = new_buffer.tell()

            new_buffer.seek(0)
            data = new_buffer.read()

        logging.debug("Uploading %s (%s bytes)" % (path, headers['Content-Length']))
        key = bucket.new_key(path)
        key.set_contents_from_string(data, headers=headers, policy='public-read', replace=True,
                                     reduced_redundancy=False)

    except Exception as e:
        import traceback
        print(traceback.format_exc(e), file=sys.stderr)
        return 0

    # Return number of bytes uploaded.
    return headers['Content-Length']


def deploy_tarball_to_s3(tarball_obj, bucket_name, prefix='', region='us-west-2', concurrency=50, no_compress=False):
    """
    Upload the contents of `tarball_obj`, a File-like object representing a valid .tar.gz file, to the S3 bucket `bucket_name`
    """
    # Connect to S3 and get a reference to the bucket name we will push files to
    conn = connect_to_region(region)
    if conn is None:
        logging.error("Invalid AWS region %s" % region)
        return

    try:
        bucket = conn.get_bucket(bucket_name, validate=True)
    except boto.exception.S3ResponseError:
        logging.error("S3 bucket %s does not exist in region %s" % (bucket_name, region))
        return

    # Open the tarball
    try:
        with tarfile.open(name=None, mode="r:*", fileobj=tarball_obj) as tarball:

            files_uploaded = 0

            # Parallelize the uploads so they don't take ages
            pool = Pool(concurrency)

            # Iterate over the tarball's contents.
            try:
                for member in tarball:

                    # Ignore directories, links, devices, fifos, etc.
                    if not member.isfile():
                        continue

                    path = os.path.join(prefix, member.name)

                    # Read file data from the tarball
                    fd = tarball.extractfile(member)

                    # Send a job to the process pool.
                    pool.wait_available()
                    pool.apply_async(__deploy_asset_to_s3, (fd.read(), path, member.size, bucket, not no_compress))

                    files_uploaded += 1

                # Wait for all transfers to finish
                pool.join()

            except KeyboardInterrupt:
                # Ctrl-C pressed
                print("Cancelling upload...")
                pool.join()

            finally:
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
    parser.add_argument("filename", type=str, help="File to load")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    with open(args.filename) as fd:
        deploy_tarball_to_s3(fd, args.bucket, args.prefix, args.region, args.concurrency, args.no_compress)


if __name__ == "__main__":
    main()
