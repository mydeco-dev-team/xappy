# Copyright (C) 2009 Richard Boulton
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
from xappytest import *
from xappy.cachemanager import *
import random

class TestCachedSearches(TestCase):
    def pre_test(self):
        self.cachepath = os.path.join(self.tempdir, 'cache')
        self.dbpath = os.path.join(self.tempdir, 'db')
        self.doccount = 120

    def test_xapian_cache(self):
        random.seed(42)

        # Make a database, and add some documents to it.
        iconn = xappy.IndexerConnection(self.dbpath)
        iconn.add_field_action('text', xappy.FieldActions.INDEX_FREETEXT)
        for i in xrange(self.doccount):
            doc = xappy.UnprocessedDocument()
            doc.append('text', 'hello')
            if i > self.doccount / 2:
                doc.append('text', 'world')
            iconn.add(doc)

        # Make a cache, and set the hits for some queries.
        man = XapianCacheManager(self.cachepath)
        man.set_hits(man.get_or_make_queryid('hello'),
                     range(self.doccount, 0, -10))

        world_order = list(xrange(1, self.doccount + 1))
        random.shuffle(world_order)
        man.set_hits(man.get_or_make_queryid('world'), world_order)

        # Apply the cache to the index.
        iconn.set_cache_manager(man)
        iconn.apply_cached_items()
        iconn.flush()

        iconn.delete('10')
        iconn.flush()
        iconn.delete(xapid=50)

        doc = xappy.UnprocessedDocument()
        doc.append('text', 'hello')
        doc.id = hex(50)[2:]
        iconn.replace(doc)
        doc.id = hex(20)[2:]
        iconn.replace(doc, xapid=21)
        iconn.flush()

        # Try a plain search.
        sconn = xappy.SearchConnection(self.dbpath)
        sconn.set_cache_manager(man)

        query_hello = sconn.query_parse('hello')
        query_world = sconn.query_parse('world')

        results = sconn.search(query_hello, 0, self.doccount)
        results = [int(result.id, 16) for result in results]
        expected = list(xrange(self.doccount))
        expected.remove(16)
        expected.remove(49)
        self.assertEqual(results, expected)

        # Try a search with a cache.
        cached_hello = sconn.query_cached(man.get_queryid('hello'))
        results = sconn.search(query_hello.norm() | cached_hello, 0, self.doccount)
        results = [int(result.id, 16) for result in results]
        expected2 = list(xrange(self.doccount - 1, 0, -10))
        expected2.remove(49)
        self.assertEqual(results[:11], expected2)
        self.assertEqual(list(sorted(results)), expected)

        # Try the same search with a different set of cached results.
        cached_world = sconn.query_cached(man.get_queryid('world'))
        results = sconn.search(query_hello.norm() | cached_world, 0, self.doccount)
        results = [int(result.id, 16) for result in results]
        world_order.remove(17)
        world_order.remove(50)
        self.assertEqual(results, [i - 1 for i in world_order])
        self.assertEqual(list(sorted(results)), expected)

        # Try another search with a cache.
        results = sconn.search(query_world.norm() | cached_world, 0, self.doccount)
        results = [int(result.id, 16) for result in results]
        self.assertEqual(results, [i - 1 for i in world_order])


if __name__ == '__main__':
    main()
