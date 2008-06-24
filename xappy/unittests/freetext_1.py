# Copyright (C) 2008 Lemur Consulting Ltd
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
from xappytest import *

class TestFreeText(TestCase):
    def pre_test(self):
        self.indexpath = os.path.join(self.tempdir, 'foo')
        iconn = xappy.IndexerConnection(self.indexpath)
        iconn.add_field_action('a', xappy.FieldActions.INDEX_FREETEXT)
        iconn.add_field_action('b', xappy.FieldActions.INDEX_FREETEXT, search_by_default=False)
        iconn.add_field_action('c', xappy.FieldActions.INDEX_FREETEXT, search_by_default=True)
        iconn.add_field_action('d', xappy.FieldActions.INDEX_FREETEXT, allow_field_specific=False)
        iconn.add_field_action('e', xappy.FieldActions.INDEX_FREETEXT, allow_field_specific=True)
        iconn.add_field_action('f', xappy.FieldActions.INDEX_FREETEXT, search_by_default=False,  allow_field_specific=False)

        iconn.add_field_action('a', xappy.FieldActions.STORE_CONTENT)
        iconn.add_field_action('b', xappy.FieldActions.STORE_CONTENT)
        iconn.add_field_action('c', xappy.FieldActions.STORE_CONTENT)

        for i in xrange(32):
            doc = xappy.UnprocessedDocument()
            if i % 2:
                doc.fields.append(xappy.Field('a', 'termA'))
            if (i / 2) % 2:
                doc.fields.append(xappy.Field('b', 'termB'))
            if (i / 4) % 2:
                doc.fields.append(xappy.Field('c', 'termC'))
            if (i / 8) % 2:
                doc.fields.append(xappy.Field('d', 'termD'))
            if (i / 16) % 2:
                doc.fields.append(xappy.Field('e', 'termE'))
            if (i / 3) % 3 == 0:
                doc.fields.append(xappy.Field('f', 'termF'))
            iconn.add(doc)

        iconn.flush()
        iconn.close()
        self.sconn = xappy.SearchConnection(self.indexpath)

    def post_test(self):
        self.sconn.close()

    def test_search_by_default1(self):
        # Search by default (due to default handling)
        q = self.sconn.query_parse('termA')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31]))

        # Not searched by default
        q = self.sconn.query_parse('termB')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([]))

        # Explicitly searched by default
        q = self.sconn.query_parse('termC')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31]))

        # Not searched by default
        q = self.sconn.query_parse('termF')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([]))


    def test_search_by_default2(self):
        # Search by default (due to default handling)
        q = self.sconn.query_parse('termA', default_allow=('a', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31]))

        # Not searched by default, but can be explicitly specified
        q = self.sconn.query_parse('termB', default_allow=('b', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([2, 3, 6, 7, 10, 11, 14, 15, 18, 19, 22, 23, 26, 27, 30, 31]))

        # Explicitly searched by default
        q = self.sconn.query_parse('termC', default_allow=('c', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23, 28, 29, 30, 31]))

        # Not searched by default, by can't be explicitly specified either
        # because it's not indexed as allow_field_specific
        q = self.sconn.query_parse('termF')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([]))

    def test_allow_field_specific1(self):
        # Search by default (due to default handling)
        q = self.sconn.query_parse('a:termA', allow=('a', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31]))

        # Not indexed for field specific searching
        q = self.sconn.query_parse('d:termD', allow=('d', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([]))

        # Explicitly indexed for field specific searching
        q = self.sconn.query_parse('e:termE', allow=('e', ))
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]))

        # Not indexed for field specific searching
        q = self.sconn.query_parse('termF')
        res = self.sconn.search(q, 0, 100)
        self.assertEqual(set([int(item.id, 16) for item in res]), set([]))

if __name__ == '__main__':
    main()
