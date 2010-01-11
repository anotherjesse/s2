#!/usr/bin/env python

# Copyright (c) 2006,2007 Mitch Garnaat http://garnaat.org/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

"""
Some unit tests for the S3Connection
"""

import unittest
import time
import os
import boto
import random
from boto.s3.connection import S3Connection
from boto.exception import S3PermissionsError

class S3ConnectionTest (unittest.TestCase):
    
    def setUp(self):
        self.conn = S3Connection(aws_secret_access_key="foo",
                                 aws_access_key_id="bar",
                                 is_secure=False,
                                 debug=1,
                                 port=8000,
                                 host="127.0.0.1",
                                 calling_format=boto.s3.connection.OrdinaryCallingFormat())
        bucket_name = 'test-%s-%d' % (random.random(), int(time.time()))
        self.bucket = self.conn.create_bucket(bucket_name)
        self.fn = "foobar-%s" % random.random()
    
    def tearDown(self):
        for k in self.bucket.get_all_keys():
            self.bucket.delete_key(k)
        self.conn.delete_bucket(self.bucket)

    def test_1_basic(self):
        c = self.conn
        # now try a get_bucket call and see if it's really there
        bucket = c.get_bucket(self.bucket.name)
        # create a new key and store it's content from a string
        k = bucket.new_key()
        k.name = 'foobar'
        s1 = 'This is a test of file upload and download'
        s2 = 'This is a second string to test file upload and download'
        k.set_contents_from_string(s1)
        fp = open(self.fn, 'wb')
        # now get the contents from s3 to a local file
        k.get_contents_to_file(fp)
        fp.close()
        fp = open(self.fn)
        # check to make sure content read from s3 is identical to original
        assert s1 == fp.read(), 'corrupted file'
        fp.close()
        bucket.delete_key(k)
        # test a few variations on get_all_keys - first load some data
        # for the first one, let's override the content type
        phony_mimetype = 'application/x-boto-test'
        headers = {'Content-Type': phony_mimetype}
        k.name = 'foo/bar'
        k.set_contents_from_string(s1, headers)
        k.name = 'foo/bas'
        k.set_contents_from_filename(self.fn)
        k.name = 'foo/bat'
        k.set_contents_from_string(s1)
        k.name = 'fie/bar'
        k.set_contents_from_string(s1)
        k.name = 'fie/bas'
        k.set_contents_from_string(s1)
        k.name = 'fie/bat'
        k.set_contents_from_string(s1)
        # try resetting the contents to another value
        md5 = k.md5
        k.set_contents_from_string(s2)
        assert k.md5 != md5
        os.unlink(self.fn)
        all = bucket.get_all_keys()
        assert len(all) == 6
        rs = bucket.get_all_keys(prefix='foo')
        assert len(rs) == 3
        rs = bucket.get_all_keys(prefix='', delimiter='/')
        assert len(rs) == 2
        rs = bucket.get_all_keys(maxkeys=5)
        assert len(rs) == 5
        # test the lookup method
        k = bucket.lookup('foo/bar')
        assert isinstance(k, bucket.key_class)
        assert k.content_type == phony_mimetype
        k = bucket.lookup('notthere')
        assert k == None
        # try a key with a funny character
        rs = bucket.get_all_keys()
        num_keys = len(rs)
        k = bucket.new_key()
        k.name = 'testnewline\n'
        k.set_contents_from_string('This is a test')
        rs = bucket.get_all_keys()
        assert len(rs) == num_keys + 1
        bucket.delete_key(k)
        rs = bucket.get_all_keys()
        assert len(rs) == num_keys
        
    def test_meta_data(self):
        bucket = self.bucket
        # try some metadata stuff
        k = bucket.new_key()
        k.name = 'has_metadata'
        mdkey1 = 'meta1'
        mdval1 = 'This is the first metadata value'
        k.set_metadata(mdkey1, mdval1)
        mdkey2 = 'meta2'
        mdval2 = 'This is the second metadata value'
        k.set_metadata(mdkey2, mdval2)
        k.set_contents_from_string('content')
        k = bucket.lookup('has_metadata')
        assert k.get_metadata(mdkey1) == mdval1
        assert k.get_metadata(mdkey2) == mdval2
        k = bucket.new_key()
        k.name = 'has_metadata'
        k.get_contents_as_string()
        assert k.get_metadata(mdkey1) == mdval1
        assert k.get_metadata(mdkey2) == mdval2
        bucket.delete_key(k)
    
        
    # def test_acl(self):
        # bucket.set_acl('public-read')
        # policy = bucket.get_acl()
        # assert len(policy.acl.grants) == 2
        # bucket.set_acl('private')
        # policy = bucket.get_acl()
        # assert len(policy.acl.grants) == 1
        # k = bucket.lookup('foo/bar')
        # k.set_acl('public-read')
        # policy = k.get_acl()
        # assert len(policy.acl.grants) == 2
        # k.set_acl('private')
        # policy = k.get_acl()
        # assert len(policy.acl.grants) == 1
        # # try the convenience methods for grants
        # bucket.add_user_grant('FULL_CONTROL',
        #                       'c1e724fbfa0979a4448393c59a8c055011f739b6d102fb37a65f26414653cd67')
        # try:
        #     bucket.add_email_grant('foobar', 'foo@bar.com')
        # except S3PermissionsError:
        #     pass

    def test_2_1mb_upload(self):
        k = self.bucket.new_key()
        k.name = 'foobar'
        k.set_contents_from_filename("1mb")
        fp = open(self.fn, 'wb')
        k.get_contents_to_file(fp)
        fp.close()
        # check to make sure content read from s3 is identical to original
        assert open(self.fn).read() == open('1mb').read()
