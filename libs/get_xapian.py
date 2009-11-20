#!/usr/bin/env python
#
# Copyright (C) 2007 Lemur Consulting Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
r"""get_xapian.py: Download and unpack the xapian archives.

"""
__docformat__ = "restructuredtext en"

import copy
import glob
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib2

try:
    import hashlib
    make_sha1_hasher = hashlib.sha1
except ImportError:
    import sha
    make_sha1_hasher = sha.new
# List of the archives.
#
# The values are, in order:
#  - Descriptive name of archive
#  - URL to download
#  - Filename to store downloaded package as
#  - SHA1 sum of package
archives = [
    ('Xapian core',
     'http://xappy.googlecode.com/files/xapian-core-13655.tgz',
     'xapian-core.tgz',
     '103f9ace5bc6f1ab131b17bee98496dd614e57a3',
     '',
    ),
    ('Xapian bindings',
     'http://xappy.googlecode.com/files/xapian-bindings-13655.tgz',
     'xapian-bindings.tgz',
     'e72a1b67c1c4953c7627a8df6e322215cbed0114',
     '',
    ),
    ('Xapian win32 build system',
     'http://xappy.googlecode.com/files/win32msvc-13655.tgz',
     'win32msvc.tgz',
     '63fe4cd29dd9064b90f3c34ce034e7a3e256af91',
     'xapian-core/win32',
    ),
    ('Xapian extras',
     'http://xappy.googlecode.com/files/xapian-extras-12302.tgz',
     'xapian-extras.tgz',
     '3e2ef5fcf83eb3eae1ac6e99436e40ca501dddae',
     '',
    ),
    ('Xapian extras bindings',
     'http://xappy.googlecode.com/files/xapian-extras-bindings-12302.tgz',
     'xapian-extras-bindings.tgz',
     '20806b9c47753fbc5c7c99356ed617fa06f81afb',
     '',
    ),
]

def get_script_dir():
    """Get the path of the directory containing this script.

    """
    global scriptdir
    if 'scriptdir' not in globals():
        scriptdir = os.path.dirname(os.path.abspath(__file__))
    return scriptdir

def get_package_dir():
    """Get the path to store the downloaded packages in.

    This is a standard location relative to this script.

    """
    return os.path.abspath(os.path.join(get_script_dir(), '..', 'libs'))

def calc_sha_hash(filepath):
    """Calculate the SHA1 hash of the file at the given path.

    """
    hasher = make_sha1_hasher()
    fd = open(filepath, 'rb', 0)
    try:
        while True:
            chunk = fd.read(65536)
            if len(chunk) == 0:
                break
            hasher.update(chunk)
    finally:
        fd.close()
    return hasher.hexdigest()

def download_file(url, destpath):
    """Download a file, and place it in destpath.

    """
    destdir = os.path.dirname(destpath)
    if not os.path.isdir(destdir):
        os.makedirs(destdir)

    fd = urllib2.urlopen(url)
    tmpfd, tmpname = tempfile.mkstemp(dir=destdir, prefix='xappy')
    try:
        os.write(tmpfd, fd.read())
        os.close(tmpfd)
        os.rename(tmpname, destpath)
    finally:
        if os.path.exists(tmpname):
            os.unlink(tmpname)

def unpack_tar_archive(filename, tempdir):
    """Unpack the tar archive at filename.

    Puts the contents in a directory with basename tempdir.

    """
    tf = tarfile.open(filename)
    try:
        dirname = None
        for member in tf.getmembers():
            topdir = member.name.split('/', 1)[0]
            if dirname is None:
                dirname = topdir
            else:
                if dirname != topdir:
                    raise ValueError('Archive has multiple toplevel directories: %s and %s' % (topdir, dirname))
            tf.extract(member, path=tempdir)
        return os.path.join(tempdir, dirname)
    finally:
        tf.close()

def get_archive_from_url(name, url, archivename, expected_hash):
    """Download an archive from the specified URL.

    Returns the path the archive was downloaded to, or None if
    the archive couldn't be downloaded

    """
    print("Checking for %s" % name)

    # Get the path that the package should be downloaded to
    filepath = os.path.join(package_dir, archivename)

    # Check if the package is already downloaded (and has correct SHA key).
    if os.path.exists(filepath):
        calculated_hash = calc_sha_hash(filepath)
        if expected_hash != calculated_hash:
            print("Package of %s at '%s' has wrong hash (probably an old version) - discarding" % (name, archivename))
            print("(Got %s, expected %s)" % (calculated_hash, expected_hash))
            os.unlink(filepath)

    # Download the package if needed.
    if not os.path.exists(filepath):
        print("Downloading %s from %s" % (name, url))
        download_file(url, filepath)
        calculated_hash = calc_sha_hash(filepath)
        if expected_hash != calculated_hash:
            print("Package of %s at '%s' has wrong hash - cannot continue" % (name, archivename))
            print("(Got %s, expected %s)" % (calculated_hash, expected_hash))
            os.unlink(filepath)
            return None

    return filepath

def get_archives(archives):
    """Download and unpack the xapian archives.

    """
    package_dir = get_package_dir()
    for name, url, archivename, expected_hash, target_location in archives:
        archivepath = get_archive_from_url(name, url, archivename, expected_hash)
        if archivepath is None:
            return False

        print("Unpacking %s" % name)
        archivedir = unpack_tar_archive(archivepath, package_dir)

        if target_location != '':
            target_path = os.path.join(package_dir, target_location)
            if os.path.exists(target_path):
                print("Removing old unpacked copy of archive from %s" %
                      target_location)
                shutil.rmtree(target_path)

            print("Moving %s to %s" % (name, target_location))
            shutil.move(archivedir, target_path)

    return True

def make_file_writable(filename):
    if os.name == 'nt':
        import win32api, win32con
        x = win32api.GetFileAttributes(filename)
        x &= ~win32con.FILE_ATTRIBUTE_READONLY
        win32api.SetFileAttributes(filename, x)
    else:
        os.chmod(filename, 700)

def make_tree_writable(root):
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            make_file_writable(filepath)

if __name__ == '__main__':
    package_dir = get_package_dir()
    if not get_archives(archives):
        sys.exit(1)
    sys.exit(0)
