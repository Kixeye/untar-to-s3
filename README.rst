===========
untar-to-s3
===========

*Utility script for efficiently unpacking a tarball to an S3 bucket.*

The script can load most tar files (e.g. .tar, .tar.gz) and uploads all files to an S3 bucket with an optional
prefix.

The script will automatically gzip certain file types, and will add a 'Cache-Control' header.

Requirements::
    Python 2.7 and above
    boto3 library (to install: sudo pip install boto3)

Recommended::
    gevent library to parallelize uploads to S3 (to install: sudo pip install gevent)

For usage overview::

    python untar-to-s3.py -h

Example::

    export AWS_ACCESS_KEY_ID=<iam account with PubObject permission>
    export AWS_SECRET_ACCESS_KEY=<secret from above account>
    python untar-to-s3.py web-assets-1.2.23.tar.gz --bucket my-bucket-name --prefix production

