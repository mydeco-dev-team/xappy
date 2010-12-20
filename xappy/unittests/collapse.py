# vim: set fileencoding=utf-8 :
#
# Copyright (C) 2010 Richard Boulton
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

class TestCollapse(TestCase):
    def pre_test(self):
        self.indexpath = os.path.join(self.tempdir, 'foo')
        iconn = xappy.IndexerConnection(self.indexpath)
        iconn.add_field_action('key1', xappy.FieldActions.COLLAPSE)
        iconn.add_field_action('key2', xappy.FieldActions.COLLAPSE)

        for i in xrange(10):
            doc = xappy.UnprocessedDocument()
            doc.append('key1', str(i % 5))
            doc.append('key2', str(i % 7))
            iconn.add(doc)
        iconn.flush()
        iconn.close()
        self.sconn = xappy.SearchConnection(self.indexpath)

    def post_test(self):
        self.sconn.close()

    def _search(self, **kwargs):
        res = self.sconn.query_all().search(0, 100, **kwargs)
        return [(int(item.id), item.collapse_count, item.collapse_key)
                for item in res]

    def test_collapse(self):
        """Test that collapsing works, and gives the appropriate counts.

        """
        self.assertEqual(self._search(),
                         [(0, 0, ''),
                          (1, 0, ''),
                          (2, 0, ''),
                          (3, 0, ''),
                          (4, 0, ''),
                          (5, 0, ''),
                          (6, 0, ''),
                          (7, 0, ''),
                          (8, 0, ''),
                          (9, 0, ''),
                         ])

        self.assertEqual(self._search(collapse='key1'),
                         [(0, 1, '0'),
                          (1, 1, '1'),
                          (2, 1, '2'),
                          (3, 1, '3'),
                          (4, 1, '4'),
                         ])

        self.assertEqual(self._search(collapse='key1', collapse_max=0),
                         [(0, 0, ''),
                          (1, 0, ''),
                          (2, 0, ''),
                          (3, 0, ''),
                          (4, 0, ''),
                          (5, 0, ''),
                          (6, 0, ''),
                          (7, 0, ''),
                          (8, 0, ''),
                          (9, 0, ''),
                         ])

        self.assertEqual(self._search(collapse='key1', collapse_max=1),
                         [(0, 1, '0'),
                          (1, 1, '1'),
                          (2, 1, '2'),
                          (3, 1, '3'),
                          (4, 1, '4'),
                         ])

        self.assertEqual(self._search(collapse='key1', collapse_max=2),
                         [(0, 0, '0'),
                          (1, 0, '1'),
                          (2, 0, '2'),
                          (3, 0, '3'),
                          (4, 0, '4'),
                          (5, 0, '0'),
                          (6, 0, '1'),
                          (7, 0, '2'),
                          (8, 0, '3'),
                          (9, 0, '4'),
                         ])

if __name__ == '__main__':
    main()
