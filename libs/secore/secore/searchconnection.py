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
r"""searchconnection.py: A connection to the search engine for searching.

"""
__docformat__ = "restructuredtext en"

import os as _os
import cPickle as _cPickle

import xapian as _xapian
from datastructures import *
from fieldactions import *
import fieldmappings as _fieldmappings
import highlight as _highlight 
import errors as _errors
import indexerconnection as _indexerconnection

class SearchResult(ProcessedDocument):
    """A result from a search.

    """
    def __init__(self, msetitem, results):
        ProcessedDocument.__init__(self, results._fieldmappings, msetitem.document)
        self.rank = msetitem.rank
        self._results = results

    def _get_language(self, field):
        """Get the language that should be used for a given field.

        """
        try:
            actions = self._results._conn._field_actions[field]._actions
        except KeyError:
            actions = {}
        for action, kwargslist in actions.iteritems():
            if action == FieldActions.INDEX_FREETEXT:
                for kwargs in kwargslist:
                    try:
                        return kwargs['language']
                    except KeyError:
                        pass
        return 'none'

    def summarise(self, field, maxlen=600, hl=('<b>', '</b>')):
        """Return a summarised version of the field specified.

        This will return a summary of the contents of the field stored in the
        search result, with words which match the query highlighted.

        The maximum length of the summary (in characters) may be set using the
        maxlen parameter.

        The return value will be a string holding the summary, with
        highlighting applied.  If there are multiple instances of the field in
        the document, the instances will be joined with a newline character.
        
        To turn off highlighting, set hl to None.  Each highlight will consist
        of the first entry in the `hl` list being placed before the word, and
        the second entry in the `hl` list being placed after the word.

        Any XML or HTML style markup tags in the field will be stripped before
        the summarisation algorithm is applied.

        """
        highlighter = _highlight.Highlighter(language_code=self._get_language(field))
        field = self.data[field]
        results = []
        text = '\n'.join(field)
        return highlighter.makeSample(text, self._results._query, maxlen, hl)

    def highlight(self, field, hl=('<b>', '</b>'), strip_tags=False):
        """Return a highlighted version of the field specified.

        This will return all the contents of the field stored in the search
        result, with words which match the query highlighted.

        The return value will be a list of strings (corresponding to the list
        of strings which is the raw field data).

        Each highlight will consist of the first entry in the `hl` list being
        placed before the word, and the second entry in the `hl` list being
        placed after the word.

        If `strip_tags` is True, any XML or HTML style markup tags in the field
        will be stripped before highlighting is applied.

        """
        highlighter = _highlight.Highlighter(language_code=self._get_language(field))
        field = self.data[field]
        results = []
        for text in field:
            results.append(highlighter.highlight(text, self._results._query, hl, strip_tags))
        return results

    def __repr__(self):
        return ('<SearchResult(rank=%d, id=%r, data=%r)>' %
                (self.rank, self.id, self.data))


class SearchResultIter(object):
    """An iterator over a set of results from a search.

    """
    def __init__(self, results):
        self._results = results
        self._iter = iter(results._mset)

    def next(self):
        msetitem = self._iter.next()
        return SearchResult(msetitem, self._results)


class SearchResults(object):
    """A set of results of a search.

    """
    def __init__(self, conn, enq, query, mset, fieldmappings, tagspy,
                 facetspy, facetfields):
        self._conn = conn
        self._enq = enq
        self._query = query
        self._mset = mset
        self._fieldmappings = fieldmappings
        self._tagspy = tagspy
        self._facetspy = facetspy
        self._facetfields = facetfields
        self._numeric_ranges_built = {}

    def __repr__(self):
        return ("<SearchResults(startrank=%d, "
                "endrank=%d, "
                "more_matches=%s, "
                "matches_lower_bound=%d, "
                "matches_upper_bound=%d, "
                "matches_estimated=%d, "
                "estimate_is_exact=%s)>" %
                (
                 self.startrank,
                 self.endrank,
                 self.more_matches,
                 self.matches_lower_bound,
                 self.matches_upper_bound,
                 self.matches_estimated,
                 self.estimate_is_exact,
                ))

    def _get_more_matches(self):
        # This check relies on us having asked for at least one more result
        # than retrieved to be checked.
        return (self.matches_lower_bound > self.endrank)
    more_matches = property(_get_more_matches, doc=
    """Check whether there are further matches after those in this result set.

    """)
    def _get_startrank(self):
        return self._mset.get_firstitem()
    startrank = property(_get_startrank, doc=
    """Get the rank of the first item in the search results.

    This corresponds to the "startrank" parameter passed to the search() method.

    """)
    def _get_endrank(self):
        return self._mset.get_firstitem() + len(self._mset)
    endrank = property(_get_endrank, doc=
    """Get the rank of the item after the end of the search results.

    If there are sufficient results in the index, this corresponds to the
    "endrank" parameter passed to the search() method.

    """)
    def _get_lower_bound(self):
        return self._mset.get_matches_lower_bound()
    matches_lower_bound = property(_get_lower_bound, doc=
    """Get a lower bound on the total number of matching documents.

    """)
    def _get_upper_bound(self):
        return self._mset.get_matches_upper_bound()
    matches_upper_bound = property(_get_upper_bound, doc=
    """Get an upper bound on the total number of matching documents.

    """)
    def _get_estimated(self):
        return self._mset.get_matches_estimated()
    matches_estimated = property(_get_estimated, doc=
    """Get an estimate for the total number of matching documents.

    """)
    def _estimate_is_exact(self):
        return self._mset.get_matches_lower_bound() == \
               self._mset.get_matches_upper_bound()
    estimate_is_exact = property(_estimate_is_exact, doc=
    """Check whether the estimated number of matching documents is exact.

    If this returns true, the estimate given by the `matches_estimated`
    property is guaranteed to be correct.

    If this returns false, it is possible that the actual number of matching
    documents is different from the number given by the `matches_estimated`
    property.

    """)

    def get_hit(self, index):
        """Get the hit with a given index.

        """
        msetitem = self._mset.get_hit(index)
        return SearchResult(msetitem, self)
    __getitem__ = get_hit

    def __iter__(self):
        """Get an iterator over the hits in the search result.

        The iterator returns the results in increasing order of rank.

        """
        return SearchResultIter(self)

    def get_top_tags(self, field, maxtags):
        """Get the most frequent tags in a given field.

         - `field` - the field to get tags for.  This must have been specified
           in the "gettags" argument of the search() call.
         - `maxtags` - the maximum number of tags to return.

        Returns a sequence of 2-item tuples, in which the first item in the
        tuple is the tag, and the second is the frequency of the tag in the
        matches seen (as an integer).

        """
        if self._tagspy is None:
            raise _errors.SearchError("Field %r was not specified for getting tags" % field)
        try:
            prefix = self._conn._field_mappings.get_prefix(field)
        except KeyError:
            raise _errors.SearchError("Field %r was not indexed for tagging" % field)
        return self._tagspy.get_top_terms(prefix, maxtags)

    def get_suggested_facets(self, maxfacets=5, desired_num_of_categories=7):
        """Get a suggested set of facets, to present to the user.

        This returns a list, in descending order of the usefulness of the
        facet, in which each item is a tuple holding:

         - fieldname of facet.
         - sequence of 2-tuples holding the suggested values or ranges for that
           field:

           For facets of type 'string', the first item in the 2-tuple will
           simply be the string supplied when the facet value was added to its
           document.  For facets of type 'float', it will be a 2-tuple, holding
           floats giving the start and end of the suggested value range.

           The second item in the 2-tuple will be the frequency of the facet
           value or range in the result set.

        """
        if self._facetspy is None:
            return []
        scores = []
        facettypes = {}
        for field, slot, kwargslist in self._facetfields:
            type = None
            for kwargs in kwargslist:
                type = kwargs.get('type', None)
                if type is not None: break
            if type is None: type = 'string'

            if type == 'float':
                if field not in self._numeric_ranges_built:
                    field, self._facetspy.build_numeric_ranges(slot, desired_num_of_categories)
                    self._numeric_ranges_built[field] = None
            facettypes[field] = type
            score = self._facetspy.score_categorisation(slot,
                                                        desired_num_of_categories)
            scores.append((score, field, slot))
        scores.sort()

        result = []
        for score, field, slot in scores:
            values = self._facetspy.get_values_as_dict(slot)
            if len(values) <= 1:
                continue
            newvalues = []
            if facettypes[field] == 'float':
                # Convert numbers to python numbers, and number ranges to a
                # python tuple of two numbers.
                for value, frequency in values.iteritems():
                    if len(value) <= 9:
                        value1 = _xapian.sortable_unserialise(value)
                        value2 = value1
                    else:
                        value1 = _xapian.sortable_unserialise(value[:9])
                        value2 = _xapian.sortable_unserialise(value[9:])
                    newvalues.append(((value1, value2), frequency))
            else:
                for value, frequency in values.iteritems():
                    newvalues.append((value, frequency))
                
            newvalues.sort()
            result.append((field, newvalues))
            if len(result) >= maxfacets:
                break
        return result
        

class SearchConnection(object):
    """A connection to the search engine for searching.

    The connection will access a view of the database.

    """
    _qp_flags_std = (_xapian.QueryParser.FLAG_PHRASE |
                     _xapian.QueryParser.FLAG_BOOLEAN |
                     _xapian.QueryParser.FLAG_LOVEHATE |
                     _xapian.QueryParser.FLAG_AUTO_SYNONYMS |
                     _xapian.QueryParser.FLAG_AUTO_MULTIWORD_SYNONYMS)
    _qp_flags_nobool = (_qp_flags_std | _xapian.QueryParser.FLAG_BOOLEAN) ^ _xapian.QueryParser.FLAG_BOOLEAN

    def __init__(self, indexpath):
        """Create a new connection to the index for searching.

        There may only an arbitrary number of search connections for a
        particular database open at a given time (regardless of whether there
        is a connection for indexing open as well).

        If the database doesn't exist, an exception will be raised.

        """
        self._index = _xapian.Database(indexpath)
        self._indexpath = indexpath

        # Read the actions.
        self._load_config()

    def _get_sort_type(self, field):
        """Get the sort type that should be used for a given field.

        """
        try:
            actions = self._field_actions[field]._actions
        except KeyError:
            actions = {}
        for action, kwargslist in actions.iteritems():
            if action == FieldActions.SORT_AND_COLLAPSE:
                for kwargs in kwargslist:
                    return kwargs['type']

    def _load_config(self):
        """Load the configuration for the database.

        """
        # Note: this code is basically duplicated in the IndexerConnection
        # class.  Move it to a shared location.
        config_file = _os.path.join(self._indexpath, 'config')
        if not _os.path.exists(config_file):
            self._field_actions = {}
            self._field_mappings = _fieldmappings.FieldMappings()
            return
        fd = open(config_file)
        config_str = fd.read()
        fd.close()

        (self._field_actions, mappings, next_docid) = _cPickle.loads(config_str)
        self._field_mappings = _fieldmappings.FieldMappings(mappings)

    def reopen(self):
        """Reopen the connection.

        This updates the revision of the index which the connection references
        to the latest flushed revision.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        self._index.reopen()
        # Re-read the actions.
        self._load_config()
        
    def close(self):
        """Close the connection to the database.

        It is important to call this method before allowing the class to be
        garbage collected to ensure that the connection is cleaned up promptly.

        No other methods may be called on the connection after this has been
        called.  (It is permissible to call close() multiple times, but
        only the first call will have any effect.)

        If an exception occurs, the database will be closed, but changes since
        the last call to flush may be lost.

        """
        if self._index is None:
            return
        # There is currently no "close()" method for xapian databases, so
        # we have to rely on the garbage collector.  Since we never copy
        # the _index property out of this class, there should be no cycles,
        # so the standard python implementation should garbage collect
        # _index straight away.  A close() method is planned to be added to
        # xapian at some point - when it is, we should call it here to make
        # the code more robust.
        self._index = None
        self._indexpath = None
        self._field_actions = None
        self._field_mappings = None

    def get_doccount(self):
        """Count the number of documents in the database.

        This count will include documents which have been added or removed but
        not yet flushed().

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        return self._index.get_doccount()

    def get_document(self, id):
        """Get the document with the specified unique ID.

        Raises a KeyError if there is no such document.  Otherwise, it returns
        a ProcessedDocument.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        postlist = self._index.postlist('Q' + id)
        try:
            plitem = postlist.next()
        except StopIteration:
            # Unique ID not found
            raise KeyError('Unique ID %r not found' % id)
        try:
            postlist.next()
            raise _errors.SearchError("Multiple documents " #pragma: no cover
                                      "found with same unique ID")
        except StopIteration:
            # Only one instance of the unique ID found, as it should be.
            pass

        result = ProcessedDocument(self._field_mappings)
        result.id = id
        result._doc = self._index.get_document(plitem.docid)
        return result

    OP_AND = _xapian.Query.OP_AND
    OP_OR = _xapian.Query.OP_OR
    def query_composite(self, operator, queries):
        """Build a composite query from a list of queries.

        The queries are combined with the supplied operator, which is either
        SearchConnection.OP_AND or SearchConnection.OP_OR.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        return _xapian.Query(operator, list(queries))

    def query_filter(self, query, filter, exclude=False):
        """Filter a query with another query.

        If exclude is False (or not specified), documents will only match the
        resulting query if they match the both the first and second query: the
        results of the first query are "filtered" to only include those which
        also match the second query.

        If exclude is True, documents will only match the resulting query if
        they match the first query, but not the second query: the results of
        the first query are "filtered" to only include those which do not match
        the second query.
        
        Documents will always be weighted according to only the first query.

        - `query`: The query to filter.
        - `filter`: The filter to apply to the query.
        - `exclude`: If True, the sense of the filter is reversed - only
          documents which do not match the second query will be returned. 

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        if not isinstance(filter, _xapian.Query):
            raise _errors.SearchError("Filter must be a Xapian Query object")
        if exclude:
            return _xapian.Query(_xapian.Query.OP_AND_NOT, query, filter)
        else:
            return _xapian.Query(_xapian.Query.OP_FILTER, query, filter)

    def query_range(self, field, begin, end):
        """Create a query for a range search.
        
        This creates a query which matches only those documents which have a
        field value in the specified range.

        Begin and end must be appropriate values for the field, according to
        the 'type' parameter supplied to the SORTABLE action for the field.

        The begin and end values are both inclusive - any documents with a
        value equal to begin or end will be returned (unless end is less than
        begin, in which case no documents will be returned).

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")

        sorttype = self._get_sort_type(field)
        marshaller = SortableMarshaller(False)
        fn = marshaller.get_marshall_function(field, sorttype)
        begin = fn(field, begin)
        end = fn(field, end)

        try:
            slot = self._field_mappings.get_slot(field)
        except KeyError:
            return _xapian.Query()
        return _xapian.Query(_xapian.Query.OP_VALUE_RANGE, slot, begin, end)

    def query_facet(self, field, val):
        """Create a query for a facet value.
        
        This creates a query which matches only those documents which have a
        facet value in the specified range.

        For a numeric range facet, val should be a tuple holding the start and
        end of the range.  For other facets, val should be the value to look
        for.

        The start and end values are both inclusive - any documents with a
        value equal to start or end will be returned (unless end is less than
        start, in which case no documents will be returned).

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")

        try:
            actions = self._field_actions[field]._actions
        except KeyError:
            actions = {}
        facettype = None
        for action, kwargslist in actions.iteritems():
            if action == FieldActions.FACET:
                for kwargs in kwargslist:
                    facettype = kwargs.get('type', None)
                    if facettype is not None:
                        break
            if facettype is not None:
                break

        if facettype == 'float':
            assert(len(val) == 2)
            try:
                slot = self._field_mappings.get_slot(field)
            except KeyError:
                return _xapian.Query()
            marshaller = SortableMarshaller(False)
            fn = marshaller.get_marshall_function(field, sorttype)
            begin = fn(field, val[0])
            end = fn(field, val[1])
            return _xapian.Query(_xapian.Query.OP_VALUE_RANGE, slot, begin, end)
        else:
            assert(facettype == 'string' or facettype is None)
            prefix = self._field_mappings.get_prefix(field)
            return _xapian.Query(prefix + val.lower())


    def _prepare_queryparser(self, allow, deny, default_op):
        """Prepare (and return) a query parser using the specified fields and
        operator.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        if allow is not None and deny is not None:
            raise _errors.SearchError("Cannot specify both `allow` and `deny`")
        qp = _xapian.QueryParser()
        qp.set_database(self._index)
        qp.set_default_op(default_op)

        if allow is None:
            allow = [key for key in self._field_actions]
        if deny is not None:
            allow = [key for key in allow if key not in deny]

        for field in allow:
            try:
                actions = self._field_actions[field]._actions
            except KeyError:
                actions = {}
            for action, kwargslist in actions.iteritems():
                if action == FieldActions.INDEX_EXACT:
                    # FIXME - need patched version of xapian to add exact prefixes
                    #qp.add_exact_prefix(field, self._field_mappings.get_prefix(field))
                    qp.add_prefix(field, self._field_mappings.get_prefix(field))
                if action == FieldActions.INDEX_FREETEXT:
                    qp.add_prefix(field, self._field_mappings.get_prefix(field))
                    for kwargs in kwargslist:
                        try:
                            lang = kwargs['language']
                            qp.set_stemmer(_xapian.Stem(lang))
                            qp.set_stemming_strategy(qp.STEM_SOME)
                        except KeyError:
                            pass
        return qp

    def query_parse(self, string, allow=None, deny=None, default_op=OP_AND):
        """Parse a query string.

        This is intended for parsing queries entered by a user.  If you wish to
        combine structured queries, it is generally better to use the other
        query building methods, such as `query_composite`.

        - `string`: The string to parse.
        - `allow`: A list of fields to allow in the query.
        - `deny`: A list of fields not to allow in the query.

        Only one of `allow` and `deny` may be specified.

        If any of the entries in `allow` are not present in the configuration
        for the database, or are not specified for indexing (either as
        INDEX_EXACT or INDEX_FREETEXT), they will be ignored.  If any of the
        entries in `deny` are not present in the configuration for the
        database, they will be ignored.

        Returns a Query object, which may be passed to the search() method, or
        combined with other queries.

        """
        qp = self._prepare_queryparser(allow, deny, default_op)
        try:
            return qp.parse_query(string, self._qp_flags_std)
        except _xapian.QueryParserError, e:
            # If we got a parse error, retry without boolean operators (since
            # these are the usual cause of the parse error).
            return qp.parse_query(string, self._qp_flags_nobool)

    def query_field(self, field, value, default_op=OP_AND):
        """A query for a single field.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        try:
            actions = self._field_actions[field]._actions
        except KeyError:
            actions = {}

        # need to check on field type, and stem / split as appropriate
        for action, kwargslist in actions.iteritems():
            if action in (FieldActions.INDEX_EXACT,
                          FieldActions.TAG,
                          FieldActions.FACET,):
                prefix = self._field_mappings.get_prefix(field)
                if len(value) > 0:
                    chval = ord(value[0])
                    if chval >= ord('A') and chval <= ord('Z'):
                        prefix = prefix + ':'
                return _xapian.Query(prefix + value)
            if action == FieldActions.INDEX_FREETEXT:
                qp = _xapian.QueryParser()
                qp.set_default_op(default_op)
                prefix = self._field_mappings.get_prefix(field)
                for kwargs in kwargslist:
                    try:
                        lang = kwargs['language']
                        qp.set_stemmer(_xapian.Stem(lang))
                        qp.set_stemming_strategy(qp.STEM_SOME)
                    except KeyError:
                        pass
                return qp.parse_query(value, self._qp_flags_std, prefix)

        return _xapian.Query()

    def query_all(self):
        """A query which matches all the documents in the database.

        """
        return _xapian.Query('')

    def spell_correct(self, string, allow=None, deny=None):
        """Correct a query spelling.

        This returns a version of the query string with any misspelt words
        corrected.

        - `allow`: A list of fields to allow in the query.
        - `deny`: A list of fields not to allow in the query.

        Only one of `allow` and `deny` may be specified.

        If any of the entries in `allow` are not present in the configuration
        for the database, or are not specified for indexing (either as
        INDEX_EXACT or INDEX_FREETEXT), they will be ignored.  If any of the
        entries in `deny` are not present in the configuration for the
        database, they will be ignored.

        """
        qp = self._prepare_queryparser(allow, deny, self.OP_AND)
        qp.parse_query(string, self._qp_flags_std | qp.FLAG_SPELLING_CORRECTION)
        corrected = qp.get_corrected_query_string()
        if len(corrected) == 0:
            if isinstance(string, unicode):
                # Encode as UTF-8 for consistency - this happens automatically
                # to values passed to Xapian.
                return string.encode('utf-8')
            return string
        return corrected

    def search(self, query, startrank, endrank,
               checkatleast=0, sortby=None, collapse=None,
               gettags=None,
               getfacets=None, allowfacets=None, denyfacets=None):
        """Perform a search, for documents matching a query.

        - `query` is the query to perform.
        - `startrank` is the rank of the start of the range of matching
          documents to return (ie, the result with this rank will be returned).
          ranks start at 0, which represents the "best" matching document.
        - `endrank` is the rank at the end of the range of matching documents
          to return.  This is exclusive, so the result with this rank will not
          be returned.
        - `checkatleast` is the minimum number of results to check for: the
          estimate of the total number of matches will always be exact if
          the number of matches is less than `checkatleast`.  A value of ``-1``
          can be specified for the checkatleast parameter - this has the
          special meaning of "check all matches", and is equivalent to passing
          the result of get_doccount().
        - `sortby` is the name of a field to sort by.  It may be preceded by a
          '+' or a '-' to indicate ascending or descending order
          (respectively).  If the first character is neither '+' or '-', the
          sort will be in ascending order.
        - `collapse` is the name of a field to collapse the result documents
          on.  If this is specified, there will be at most one result in the
          result set for each value of the field.
        - `gettags` is the name of a field to count tag occurrences in, or a
          list of fields to do so.
        - `getfacets` is a boolean - if True, the matching documents will be
          examined to build up a list of the facet values contained in them.
        - `allowfacets` is a list of the fieldnames of facets to consider.
        - `denyfacets` is a list of fieldnames of facets which will not be
          considered.

        If neither 'allowfacets' or 'denyfacets' is specified, all fields
        holding facets will be considered.

        """
        if self._index is None:
            raise _errors.SearchError("SearchConnection has been closed")
        if checkatleast == -1:
            checkatleast = self._index.get_doccount()

        enq = _xapian.Enquire(self._index)
        enq.set_query(query)

        if sortby is not None:
            asc = True
            if sortby[0] == '-':
                asc = False
                sortby = sortby[1:]
            elif sortby[0] == '+':
                sortby = sortby[1:]

            try:
                slotnum = self._field_mappings.get_slot(sortby)
            except KeyError:
                raise _errors.SearchError("Field %r was not indexed for sorting" % sortby)

            # Note: we invert the "asc" parameter, because xapian treats
            # "ascending" as meaning "higher values are better"; in other
            # words, it considers "ascending" to mean return results in
            # descending order.
            enq.set_sort_by_value_then_relevance(slotnum, not asc)

        if collapse is not None:
            try:
                slotnum = self._field_mappings.get_slot(collapse)
            except KeyError:
                raise _errors.SearchError("Field %r was not indexed for collapsing" % collapse)
            enq.set_collapse_key(slotnum)

        maxitems = max(endrank - startrank, 0)
        # Always check for at least one more result, so we can report whether
        # there are more matches.
        checkatleast = max(checkatleast, endrank + 1)

        # Build the matchspy.
        matchspies = []

        # First, add a matchspy for any gettags fields
        if isinstance(gettags, basestring):
            if len(gettags) != 0:
                gettags = [gettags]
        tagspy = None
        if gettags is not None and len(gettags) != 0:
            tagspy = _xapian.TermCountMatchSpy()
            for field in gettags:
                try:
                    prefix = self._field_mappings.get_prefix(field)
                    tagspy.add_prefix(prefix)
                except KeyError:
                    raise _errors.SearchError("Field %r was not indexed for tagging" % field)
            matchspies.append(tagspy)


        # add a matchspy for facet selection here.
        facetspy = None
        facetfields = []
        if getfacets:
            if allowfacets is not None and denyfacets is not None:
                raise _errors.SearchError("Cannot specify both `allowfacets` and `denyfacets`")
            if allowfacets is None:
                allowfacets = [key for key in self._field_actions]
            if denyfacets is not None:
                allowfacets = [key for key in allowfacets if key not in denyfacets]

            for field in allowfacets:
                try:
                    actions = self._field_actions[field]._actions
                except KeyError:
                    actions = {}
                for action, kwargslist in actions.iteritems():
                    if action == FieldActions.FACET:
                        slot = self._field_mappings.get_slot(field)
                        if facetspy is None:
                            facetspy = _xapian.CategorySelectMatchSpy()
                        facetspy.add_slot(slot)
                        facetfields.append((field, slot,
                                            kwargslist))
        if facetspy is not None:
            matchspies.append(facetspy)


        # Finally, build a single matchspy to pass to get_mset().
        if len(matchspies) == 0:
            matchspy = None
        elif len(matchspies) == 1:
            matchspy = matchspies[0]
        else:
            matchspy = _xapian.MultipleMatchDecider()
            for spy in matchspies:
                matchspy.append(spy)

        enq.set_docid_order(enq.DONT_CARE)

        # Repeat the search until we don't get a DatabaseModifiedError
        while True:
            try:
                mset = enq.get_mset(startrank, maxitems, checkatleast, None,
                                    None, matchspy)
                break
            except _xapian.DatabaseModifiedError, e:
                self.reopen()
        return SearchResults(self, enq, query, mset, self._field_mappings,
                             tagspy, facetspy, facetfields)

    def iter_synonyms(self, prefix=""):
        """Get an iterator over the synonyms.

         - `prefix`: if specified, only synonym keys with this prefix will be
           returned.

        The iterator returns 2-tuples, in which the first item is the key (ie,
        a 2-tuple holding the term or terms which will be synonym expanded,
        followed by the fieldname specified (or None if no fieldname)), and the
        second item is a tuple of strings holding the synonyms for the first
        item.

        These return values are suitable for the dict() builtin, so you can
        write things like:

         >>> conn = _indexerconnection.IndexerConnection('foo')
         >>> conn.add_synonym('foo', 'bar')
         >>> conn.add_synonym('foo bar', 'baz')
         >>> conn.add_synonym('foo bar', 'foo baz')
         >>> conn.flush()
         >>> conn = SearchConnection('foo')
         >>> dict(conn.iter_synonyms())
         {('foo', None): ('bar',), ('foo bar', None): ('baz', 'foo baz')}

        """
        return _indexerconnection.SynonymIter(self._index, self._field_mappings, prefix)


if __name__ == '__main__':
    import doctest, sys
    doctest.testmod (sys.modules[__name__])
