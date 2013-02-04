#!/usr/bin/env python
#
# Copyright (C) 2009 Richard Boulton
# Copyright (C) 2011 Bruno Rezende
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
r"""xapian_manager.py: Cache manager using Xapian as its storage backend.

"""
__docformat__ = "restructuredtext en"

import generic
import os
import shutil
try:
    import simplejson as json
except ImportError:
    import json
from xappy import errors
import xapian

class XapianCacheManager(generic.KeyValueStoreCacheManager):
    """A cache manager that stores the cached items in a Xapian database.

    Note: we need to change this if we need to support keys which are longer
    than 240 characters or so.  We could fix this by using a hashing scheme for
    the tail of such keys, and add some handling for collisions.

    This class uses the default implementation of iter_by_docid().  Subclasses
    provide other implementations of iter_by_docid(), which may be more
    efficient for some situations.

    Multiple caches may be used by specifiying differing id numbers for them.

    """
    def __init__(self, dbpath, chunksize=None, id='1'):
        self.dbpath = dbpath
        self.db = None
        self.writable = False
        self.id = id
        generic.KeyValueStoreCacheManager.__init__(self, chunksize)

    def __getitem__(self, key):
        if self.db is None:
            try:
                self.db = xapian.Database(self.dbpath)
            except xapian.DatabaseOpeningError: 
                if not os.path.exists(self.dbpath):
                    # Not created yet - no value.
                    return ''
                raise
            self.writable = False
        return self.db.get_metadata(key)

    def __setitem__(self, key, value):
        if self.db is None or not self.writable:
            self.db = xapian.WritableDatabase(self.dbpath,
                                              xapian.DB_CREATE_OR_OPEN)
            self.writable = True
        self.db.set_metadata(key, value)
        self.invalidate_iter_by_docid()

    def __delitem__(self, key):
        if self.db is None or not self.writable:
            self.db = xapian.WritableDatabase(self.dbpath,
                                              xapian.DB_CREATE_OR_OPEN)
            self.writable = True
        self.db.set_metadata(key, '')
        self.invalidate_iter_by_docid()

    def keys(self):
        if self.db is None:
            try:
                self.db = xapian.Database(self.dbpath)
            except xapian.DatabaseOpeningError: 
                if not os.path.exists(self.dbpath):
                    # Not created yet - no values
                    return
                raise
            self.writable = False
        for key in self.db.metadata_keys():
            if key[0].isupper():
                yield key

    def flush(self):
        if self.db is None or not self.writable:
            return
        self.db.flush()

    def close(self):
        if self.db is None:
            return
        self.db.close()
        self.db = None
        
    def remove_cached_items(self, iconn, doc, xapid):
        #print "Removing docid=%d" % xapid
        for value in doc.values():
            base_slot = cache_manager_slot_start(iconn, self.id)
            upper_slot = base_slot + self.num_cached_queries()
            if not (base_slot <= value.num < upper_slot):
                continue
            rank = int(CACHE_MANAGER_MAX_HITS -
                       xapian.sortable_unserialise(value.value))
            self.remove_hits(
                value.num - base_slot,
                ((rank, xapid),))

    def replace(self, olddoc, newdoc):
        # Copy any cached query items over to the new document.
        for value in olddoc.values():
            if value.num < BASE_CACHE_SLOT:
                continue
            newdoc.add_value(value.num, value.value)

    def apply_cached_items(self, iconn):
        """Update the index with references to cached items.
        
        This reads all the cached items from the cache manager, and applies
        them to the index.  This allows efficient lookup of the cached ranks
        when performing a search, and when deleting items from the the
        database.

        If any documents in the cache are not present in this index, they are
        silently ignored: the assumption is that in this case, the index is a
        subset of the cached database.

        """
        iconn._caches = get_caches(iconn)

        cache_id = self.id
        num_cached_queries = self.num_cached_queries()
        num_cache_slots = int(iconn.get_metadata('num_cache_slots') or '0')
        if cache_id not in iconn._caches:
            iconn._caches[cache_id] = num_cache_slots
        num_cache_slots += num_cached_queries
        iconn.set_metadata('num_cache_slots', str(num_cache_slots))

        set_caches(iconn)

        # Remember that a cache manager has been applied in the metadata, so
        # errors can be raised if it's not set during future modifications.
        iconn.set_metadata('_xappy_hascache', '1')

        myiter = self.iter_by_docid()
        for xapid, items in myiter:
            try:
                xapdoc = iconn._index.get_document(xapid)
            except xapian.DocNotFoundError:
                # Ignore the document if not found, to allow a global cache to
                # be applied to a subdatabase.
                continue
            for queryid, rank in items:
                xapdoc.add_value(cache_manager_slot_start(iconn, self.id) + queryid,
                    xapian.sortable_serialise(CACHE_MANAGER_MAX_HITS -
                                              rank))
            iconn._index.replace_document(xapid, xapdoc)

class XapianMultipleCachesManager(object):
    def __init__(self, basepath):
        self.basepath = basepath
        self.caches = {}
        self.selected_cache = None
        self._slots_info = None

    def select_cache(self, cache_id):
        self.selected_cache = self.caches[cache_id]

    def add_cache(self, cache_id):
        new_cache = XapianCacheManager(os.path.join(self.basepath, cache_id), id=cache_id)
        self.caches[cache_id] = new_cache
        self._slots_info = None

    def __getattr__(self, attr):
        return getattr(self.selected_cache, attr)
    
    def _get_slots_info(self, iconn):
        if not self.caches:
            return None
        if self._slots_info is not None:
            return self._slots_info
        result = []
        for cache_id, cache_manager in self.caches.iteritems():
            base_slot = cache_manager_slot_start(iconn, cache_id)
            upper_slot = base_slot + cache_manager.num_cached_queries()
            result.append((base_slot, upper_slot, cache_manager))
        result.sort()
        self._slots_info = result
        return result

    def replace(self, olddoc, newdoc):
        # Copy any cached query items over to the new document.
        for value in olddoc.values():
            if value.num < BASE_CACHE_SLOT:
                continue
            newdoc.add_value(value.num, value.value)

    def remove_cached_items(self, iconn, doc, xapid):
        slots_info = self._get_slots_info(iconn)
        if not slots_info:
            return
        index = 0
        base_slot, upper_slot, cm = slots_info[index]
        for value in doc.values():
            slot_number = value.num
            if slot_number >= upper_slot:
                index += 1
                if index == len(slots_info):
                    return
                base_slot, upper_slot, cm = slots_info[index]
                
            if not (base_slot <= slot_number < upper_slot):
                continue
            rank = int(CACHE_MANAGER_MAX_HITS -
                       xapian.sortable_unserialise(value.value))
            cm.remove_hits(
            slot_number - base_slot,
            ((rank, xapid),))

    def close(self):
        if not self.caches:
            return
        for cache_manager in self.caches.itervalues():
            cache_manager.close()

    def flush(self):
        if not self.caches:
            return
        for cache_manager in self.caches.itervalues():
            cache_manager.flush()

class XapianSelfInvertingCacheManager(XapianCacheManager):
    """Cache manager using Xapian both as a key-value store, and as a mechanism
    for implementing the inversion process required by iter_by_docid.

    """
    def __init__(self, *args, **kwargs):
        XapianCacheManager.__init__(self, *args, **kwargs)
        self.inverted = False
        self.inverted_db_path = os.path.join(self.dbpath, 'inv')

    def prepare_iter_by_docid(self):
        """Prepare to iterate by document ID.
        
        This makes a Xapian database, in which each document represents a
        cached query, and is indexed by terms corresponding to the document IDs
        of the making terms.

        This is used to get the inverse of the queryid->docid list mapping
        provided to the cache.

        """
        if self.inverted:
            return

        if not os.path.exists(self.dbpath):
            self.inverted = True
            return

        shutil.rmtree(self.inverted_db_path, ignore_errors=True)
        invdb = xapian.WritableDatabase(self.inverted_db_path,
                                        xapian.DB_CREATE_OR_OPEN)
        try:
            for qid in self.iter_queryids():
                doc = xapian.Document()
                for rank, docid in enumerate(self.get_hits(qid)):
                    # We store the docid encoded as the term (encoded such that
                    # it will sort lexicographically into numeric order), and
                    # the rank as the wdf.
                    term = '%x' % docid
                    term = ('%x' % len(term)) + term
                    doc.add_term(term, rank)
                newdocid = invdb.add_document(doc)

                assert(newdocid == qid + 1)
            invdb.flush()
        finally:
            invdb.close()

        self.inverted = True

    def invalidate_iter_by_docid(self):
        if not self.inverted:
            return
        shutil.rmtree(self.inverted_db_path, ignore_errors=True)
        self.inverted = False

    def iter_by_docid(self):
        """Implementation of iter_by_docid() which uses a temporary Xapian
        database to perform the inverting of the queryid->docid list mapping,
        to return the docid->queryid list mapping.

        This uses an on-disk database, so is probably a bit slower than the
        naive implementation for small cases, but should scale arbitrarily (as
        well as Xapian does, anyway).

        It would be faster if we could tell Xapian not to perform fsyncs for
        the temporary database.

        """
        self.prepare_iter_by_docid()

        if os.path.exists(self.dbpath):
            invdb = xapian.Database(self.inverted_db_path)
        else:
            invdb = xapian.Database()

        try:

            for item in invdb.allterms():
                docid = int(item.term[1:], 16)
                items = tuple((item.docid - 1, item.wdf) for item in invdb.postlist(item.term))
                yield docid, items
            invdb.close()

        finally:
            invdb.close()

encode = lambda x: json.dumps(x, 2)
decode = json.loads

def get_caches(conn):
    """Get details of all the caches applied to a connection.

    """
    caches_meta = conn._index.get_metadata('caches')
    return decode(caches_meta) if caches_meta else {}

def set_caches(iconn):
    """Set details of all the caches applied to a connection.

    """
    iconn._index.set_metadata('caches', encode(iconn._caches))

BASE_CACHE_SLOT = 10000
# Maximum number of hits ever stored in a cache for a single query.
# This is just used to calculate an appropriate value to store for the
# weight for this item.
CACHE_MANAGER_MAX_HITS = 1000000

def cache_manager_slot_start(conn, cache_id):
    if not hasattr(conn, '_caches'):
        conn._caches = get_caches(conn)
    cache_specific_slot = conn._caches.get(cache_id, 0)
    return BASE_CACHE_SLOT + cache_specific_slot
