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
from __future__ import with_statement

import unittest
import tempfile, shutil
from contextlib import contextmanager
import os

from xappy.cachemanager import XapianMultipleCachesManager
from xappy import IndexerConnection, SearchConnection, FieldActions, Field,\
    UnprocessedDocument

@contextmanager
def tempdir(**kwargs):
    path = tempfile.mkdtemp(**kwargs)
    try:
        yield path
    finally:
        shutil.rmtree(path)



CACHE_SAMPLES = {
    '1': (
        {'queryid': 0, 'queryrepr': 'term_a', 'docids': [4, 2],},
        {'queryid': 1, 'queryrepr': 'term_b', 'docids': [2, 4],},
    ),
    'cache2': (
        {'queryid': 0, 'queryrepr': 'term_a', 'docids': [3, 4, 1],},
        {'queryid': 1, 'queryrepr': 'term_b', 'docids': [1, 4, 3],},
    ),
}

class TestMultiCacheSearchIndexLoader(unittest.TestCase):
    def _create_index(self, indexpath):
        iconn = IndexerConnection(indexpath)
        iconn.add_field_action('field', FieldActions.INDEX_FREETEXT, language='en')

        documents = [('1', [('term_a', 1), ('term_b', 5)]),
            ('2', [('term_a', 2), ('term_b', 4)]),
            ('3', [('term_a', 3), ('term_b', 3)]),
            ('4', [('term_a', 4), ('term_b', 2)]),
            ('5', [('term_a', 5), ('term_b', 1)]),
        ]

        for docid, terms in documents:
            pdoc = self._create_processed_doc(iconn, docid, terms)
            iconn.replace(pdoc, xapid=docid)
        iconn.flush()
        iconn.close()

    def _create_processed_doc(self, iconn, docid, terms):
            xappy_doc = UnprocessedDocument(docid)
            xappy_doc.fields.append(Field('field', 'term_a'))
            xappy_doc.fields.append(Field('field', 'term_b'))
            pdoc = iconn.process(xappy_doc)
            for term, wdf in terms:
                pdoc.add_term('field', term, wdf)
            return pdoc

    def _create_cache(self, cm):
        # create a cache
        cache_sample = CACHE_SAMPLES[cm.selected_cache.id]
        for querydata in cache_sample:
            cm.set_queryid(querydata['queryrepr'], querydata['queryid'])
            cm.set_hits(querydata['queryid'], querydata['docids'])
        cm.close()

    def _apply_cache(self, indexpath, cm):
        idx = IndexerConnection(indexpath)
        idx.set_cache_manager(cm)
        idx.apply_cached_items()
        idx.close()

    def _create_and_apply_cache(self, indexpath, cm):
        self._create_cache(cm)
        self._apply_cache(indexpath, cm)

    def _check_cache_results(self, indexpath, cachepath, cacheid, expected_results, num_results=10):
        # set cache manager
        cm = XapianMultipleCachesManager(cachepath)
        cm.add_cache(cacheid)
        cm.select_cache(cacheid)

        search_conn = SearchConnection(indexpath)
        search_conn.set_cache_manager(cm)

        query_id, query_term = (0, 'term_a')
        cache_query_id = cm.get_queryid(query_term) # obtain query_id from the cache
        self.assertEqual(query_id, cache_query_id)

        non_cached, cached = expected_results

        query = search_conn.query_field('field', query_term)
        base_result = [r.id for r in query.search(0, num_results)]
        # see if the results without merging the query are ok
        self.assertEqual(non_cached, base_result)

        cached_query = query.merge_with_cached(query_id)
        cached_result = [r.id for r in cached_query.search(0, num_results)]
        # test the merged query result
        self.assertEqual(cached, cached_result)

        search_conn.close()
        cm.close()

    def test_single_cache_applying(self):
        with tempdir() as basepath:

            # create an index
            indexpath = os.path.join(basepath, 'test_index')
            self._create_index(indexpath)

            # create and apply cache
            cachepath = os.path.join(basepath, 'cache')
            os.makedirs(cachepath)

            cache_manager = XapianMultipleCachesManager(cachepath)
            cache_manager.add_cache('1')
            cache_manager.select_cache('1')
            self._create_and_apply_cache(indexpath, cache_manager)

            self._check_cache_results(indexpath, cachepath, '1', [['5', '4', '3', '2', '1'], ['4', '2', '5', '3', '1']])

    def test_multiple_cache(self):
        with tempdir() as basepath:
            # create an index
            indexpath = os.path.join(basepath, 'test_index')
            self._create_index(indexpath)

            base_cachepath = os.path.join(basepath, 'cache')
            os.makedirs(base_cachepath)

            # create and apply cache 1
            cache_manager = XapianMultipleCachesManager(base_cachepath)
            cache_manager.add_cache('1')
            cache_manager.select_cache('1')
            self._create_and_apply_cache(indexpath, cache_manager)

            # create and apply cache 2
            cache_manager.add_cache('cache2')
            cache_manager.select_cache('cache2')
            self._create_and_apply_cache(indexpath, cache_manager)

            # test cache 1
            self._check_cache_results(indexpath, base_cachepath, '1', [['5', '4', '3', '2', '1'], ['4', '2', '5', '3', '1']])
            # test cache 2
            self._check_cache_results(indexpath, base_cachepath, 'cache2', [['5', '4', '3', '2', '1'], ['3', '4', '1', '5', '2']])

            # the document whose docid is 4 is in both caches, we're
            # testing here if replacing it with one cache manager set
            # will change the result in the other cache. It must change.

            # replace document
            iconn = IndexerConnection(indexpath)
            cache_manager = XapianMultipleCachesManager(base_cachepath)
            cache_manager.add_cache('1')
            cache_manager.add_cache('cache2')
            iconn.set_cache_manager(cache_manager)
            docid, terms = ('4', [('term_a', 4), ('term_b', 2)])
            pdoc = self._create_processed_doc(iconn, docid, terms)
            iconn.replace(pdoc, xapid=int(docid))
            iconn.flush()
            iconn.close()
            cache_manager.close()

            # check if the results in both caches are ok
            self._check_cache_results(indexpath, base_cachepath, '1', [['5', '4', '3', '2', '1'], ['4', '2', '5', '3', '1']])
            self._check_cache_results(indexpath, base_cachepath, 'cache2', [['5', '4', '3', '2', '1'], ['3', '4', '1', '5', '2']])

            # there are 2 code pathes when we deal with caches:
            # 1. the cache has not enough results
            # 2. the cache has enough results
            # in the first case, the result will come from a mixed query
            # against the index. In the second, the results will come from
            # the cache_manger. So, all the cache managers must be updated.

            # remove document
            cache_manager = XapianMultipleCachesManager(base_cachepath)
            cache_manager.add_cache('1')
            cache_manager.add_cache('cache2')

            iconn = IndexerConnection(indexpath)
            iconn.set_cache_manager(cache_manager)
            iconn.delete(xapid=4)
            cache_manager.close()
            iconn.flush()
            iconn.close()

            # cache has not enough results
            self._check_cache_results(indexpath, base_cachepath, '1', [['5', '3', '2', '1'], ['2', '5', '3', '1']])
            self._check_cache_results(indexpath, base_cachepath, 'cache2', [['5', '3', '2', '1'], ['3', '1', '5', '2']])

            # cache has enough results
            self._check_cache_results(indexpath, base_cachepath, '1', [['5'], ['2']], num_results=1)
            self._check_cache_results(indexpath, base_cachepath, 'cache2', [['5', '3'], ['3', '1']], num_results=2)

if __name__ == '__main__':
    unittest.main()
