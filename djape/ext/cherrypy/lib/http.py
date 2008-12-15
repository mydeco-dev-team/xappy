"""HTTP library functions."""

# This module contains functions for building an HTTP application
# framework: any one, not just one whose name starts with "Ch". ;) If you
# reference any modules from some popular framework inside *this* module,
# FuManChu will personally hang you up by your thumbs and submit you
# to a public caning.

from BaseHTTPServer import BaseHTTPRequestHandler
response_codes = BaseHTTPRequestHandler.responses.copy()

# From http://www.cherrypy.org/ticket/361
response_codes[500] = ('Internal Server Error',
                      'The server encountered an unexpected condition '
                      'which prevented it from fulfilling the request.')
response_codes[503] = ('Service Unavailable',
                      'The server is currently unable to handle the '
                      'request due to a temporary overloading or '
                      'maintenance of the server.')


import cgi
from email.Header import Header, decode_header
import re
import rfc822
HTTPDate = rfc822.formatdate
import time


def urljoin(*atoms):
    url = "/".join(atoms)
    while "//" in url:
        url = url.replace("//", "/")
    return url

def protocol_from_http(protocol_str):
    """Return a protocol tuple from the given 'HTTP/x.y' string."""
    return int(protocol_str[5]), int(protocol_str[7])

def get_ranges(headervalue, content_length):
    """Return a list of (start, stop) indices from a Range header, or None.
    
    Each (start, stop) tuple will be composed of two ints, which are suitable
    for use in a slicing operation. That is, the header "Range: bytes=3-6",
    if applied against a Python string, is requesting resource[3:7]. This
    function will return the list [(3, 7)].
    
    If this function return an empty list, you should return HTTP 416.
    """
    
    if not headervalue:
        return None
    
    result = []
    bytesunit, byteranges = headervalue.split("=", 1)
    for brange in byteranges.split(","):
        start, stop = [x.strip() for x in brange.split("-", 1)]
        if start:
            if not stop:
                stop = content_length - 1
            start, stop = map(int, (start, stop))
            if start >= content_length:
                # From rfc 2616 sec 14.16:
                # "If the server receives a request (other than one
                # including an If-Range request-header field) with an
                # unsatisfiable Range request-header field (that is,
                # all of whose byte-range-spec values have a first-byte-pos
                # value greater than the current length of the selected
                # resource), it SHOULD return a response code of 416
                # (Requested range not satisfiable)."
                continue
            if stop < start:
                # From rfc 2616 sec 14.16:
                # "If the server ignores a byte-range-spec because it
                # is syntactically invalid, the server SHOULD treat
                # the request as if the invalid Range header field
                # did not exist. (Normally, this means return a 200
                # response containing the full entity)."
                return None
            result.append((start, stop + 1))
        else:
            if not stop:
                # See rfc quote above.
                return None
            # Negative subscript (last N bytes)
            result.append((content_length - int(stop), content_length))
    
    return result


class HeaderElement(object):
    """An element (with parameters) from an HTTP header's element list."""
    
    def __init__(self, value, params=None):
        self.value = value
        if params is None:
            params = {}
        self.params = params
    
    def __unicode__(self):
        p = [";%s=%s" % (k, v) for k, v in self.params.iteritems()]
        return u"%s%s" % (self.value, "".join(p))
    
    def __str__(self):
        return str(self.__unicode__())
    
    def parse(elementstr):
        """Transform 'token;key=val' to ('token', {'key': 'val'})."""
        # Split the element into a value and parameters. The 'value' may
        # be of the form, "token=token", but we don't split that here.
        atoms = [x.strip() for x in elementstr.split(";")]
        initial_value = atoms.pop(0).strip()
        params = {}
        for atom in atoms:
            atom = [x.strip() for x in atom.split("=", 1) if x.strip()]
            key = atom.pop(0)
            if atom:
                val = atom[0]
            else:
                val = ""
            params[key] = val
        return initial_value, params
    parse = staticmethod(parse)
    
    def from_str(cls, elementstr):
        """Construct an instance from a string of the form 'token;key=val'."""
        ival, params = cls.parse(elementstr)
        return cls(ival, params)
    from_str = classmethod(from_str)


q_separator = re.compile(r'; *q *=')

class AcceptElement(HeaderElement):
    """An element (with parameters) from an Accept-* header's element list."""
    
    def from_str(cls, elementstr):
        qvalue = None
        # The first "q" parameter (if any) separates the initial
        # parameter(s) (if any) from the accept-params.
        atoms = q_separator.split(elementstr, 1)
        initial_value = atoms.pop(0).strip()
        if atoms:
            # The qvalue for an Accept header can have extensions. The other
            # headers cannot, but it's easier to parse them as if they did.
            qvalue = HeaderElement.from_str(atoms[0].strip())
        
        ival, params = cls.parse(initial_value)
        if qvalue is not None:
            params["q"] = qvalue
        return cls(ival, params)
    from_str = classmethod(from_str)
    
    def qvalue(self):
        val = self.params.get("q", "1")
        if isinstance(val, HeaderElement):
            val = val.value
        return float(val)
    qvalue = property(qvalue, doc="The qvalue, or priority, of this value.")
    
    def __cmp__(self, other):
        # If you sort a list of AcceptElement objects, they will be listed
        # in priority order; the most preferred value will be first.
        diff = cmp(other.qvalue, self.qvalue)
        if diff == 0:
            diff = cmp(str(other), str(self))
        return diff


def header_elements(fieldname, fieldvalue):
    """Return a HeaderElement list from a comma-separated header str."""
    
    if not fieldvalue:
        return None
    headername = fieldname.lower()
    
    result = []
    for element in fieldvalue.split(","):
        if headername.startswith("accept") or headername == 'te':
            hv = AcceptElement.from_str(element)
        else:
            hv = HeaderElement.from_str(element)
        result.append(hv)
    
    result.sort()
    return result

def decode_TEXT(value):
    """Decode RFC-2047 TEXT (e.g. "=?utf-8?q?f=C3=BCr?=" -> u"f\xfcr")."""
    atoms = decode_header(value)
    decodedvalue = ""
    for atom, charset in atoms:
        if charset is not None:
            atom = atom.decode(charset)
        decodedvalue += atom
    return decodedvalue

def valid_status(status):
    """Return legal HTTP status Code, Reason-phrase and Message.
    
    The status arg must be an int, or a str that begins with an int.
    
    If status is an int, or a str and  no reason-phrase is supplied,
    a default reason-phrase will be provided.
    """
    
    if not status:
        status = 200
    
    status = str(status)
    parts = status.split(" ", 1)
    if len(parts) == 1:
        # No reason supplied.
        code, = parts
        reason = None
    else:
        code, reason = parts
        reason = reason.strip()
    
    try:
        code = int(code)
    except ValueError:
        raise ValueError("Illegal response status from server "
                         "(%s is non-numeric)." % repr(code))
    
    if code < 100 or code > 599:
        raise ValueError("Illegal response status from server "
                         "(%s is out of range)." % repr(code))
    
    if code not in response_codes:
        # code is unknown but not illegal
        default_reason, message = "", ""
    else:
        default_reason, message = response_codes[code]
    
    if reason is None:
        reason = default_reason
    
    return code, reason, message


image_map_pattern = re.compile(r"[0-9]+,[0-9]+")

def parse_query_string(query_string, keep_blank_values=True):
    """Build a params dictionary from a query_string."""
    if image_map_pattern.match(query_string):
        # Server-side image map. Map the coords to 'x' and 'y'
        # (like CGI::Request does).
        pm = query_string.split(",")
        pm = {'x': int(pm[0]), 'y': int(pm[1])}
    else:
        pm = cgi.parse_qs(query_string, keep_blank_values)
        for key, val in pm.items():
            if len(val) == 1:
                pm[key] = val[0]
    return pm

def params_from_CGI_form(form):
    params = {}
    for key in form.keys():
        value_list = form[key]
        if isinstance(value_list, list):
            params[key] = []
            for item in value_list:
                if item.filename is not None:
                    value = item # It's a file upload
                else:
                    value = item.value # It's a regular field
                params[key].append(value)
        else:
            if value_list.filename is not None:
                value = value_list # It's a file upload
            else:
                value = value_list.value # It's a regular field
            params[key] = value
    return params


class CaseInsensitiveDict(dict):
    """A case-insensitive dict subclass.
    
    Each key is changed on entry to str(key).title().
    """
    
    def __getitem__(self, key):
        return dict.__getitem__(self, str(key).title())
    
    def __setitem__(self, key, value):
        dict.__setitem__(self, str(key).title(), value)
    
    def __delitem__(self, key):
        dict.__delitem__(self, str(key).title())
    
    def __contains__(self, key):
        return dict.__contains__(self, str(key).title())
    
    def get(self, key, default=None):
        return dict.get(self, str(key).title(), default)
    
    def has_key(self, key):
        return dict.has_key(self, str(key).title())
    
    def update(self, E):
        for k in E.keys():
            self[str(k).title()] = E[k]
    
    def fromkeys(cls, seq, value=None):
        newdict = cls()
        for k in seq:
            newdict[str(k).title()] = value
        return newdict
    fromkeys = classmethod(fromkeys)
    
    def setdefault(self, key, x=None):
        key = str(key).title()
        try:
            return self[key]
        except KeyError:
            self[key] = x
            return x
    
    def pop(self, key, default):
        return dict.pop(self, str(key).title(), default)


class HeaderMap(CaseInsensitiveDict):
    """A dict subclass for HTTP request and response headers.
    
    Each key is changed on entry to str(key).title(). This allows headers
    to be case-insensitive and avoid duplicates.
    
    Values are header values (decoded according to RFC 2047 if necessary).
    """
    
    def elements(self, key):
        """Return a list of HeaderElements for the given header (or None)."""
        key = str(key).title()
        h = self.get(key)
        if h is None:
            return []
        return header_elements(key, h)
    
    def output(self, protocol=(1, 1)):
        """Transform self into a list of (name, value) tuples."""
        header_list = []
        for key, v in self.iteritems():
            if isinstance(v, unicode):
                # HTTP/1.0 says, "Words of *TEXT may contain octets
                # from character sets other than US-ASCII." and
                # "Recipients of header field TEXT containing octets
                # outside the US-ASCII character set may assume that
                # they represent ISO-8859-1 characters."
                try:
                    v = v.encode("iso-8859-1")
                except UnicodeEncodeError:
                    if protocol >= (1, 1):
                        # Encode RFC-2047 TEXT
                        # (e.g. u"\u8200" -> "=?utf-8?b?6IiA?=").
                        v = Header(v, 'utf-8').encode()
                    else:
                        raise
            else:
                # This coercion should not take any time at all
                # if value is already of type "str".
                v = str(v)
            header_list.append((key, v))
        return header_list


class MaxSizeExceeded(Exception):
    pass

class SizeCheckWrapper(object):
    """Wraps a file-like object, raising MaxSizeExceeded if too large."""
    
    def __init__(self, rfile, maxlen):
        self.rfile = rfile
        self.maxlen = maxlen
        self.bytes_read = 0
    
    def _check_length(self):
        if self.maxlen and self.bytes_read > self.maxlen:
            raise MaxSizeExceeded()
    
    def read(self, size = None):
        data = self.rfile.read(size)
        self.bytes_read += len(data)
        self._check_length()
        return data
    
    def readline(self, size = None):
        if size is not None:
            data = self.rfile.readline(size)
            self.bytes_read += len(data)
            self._check_length()
            return data
        
        # User didn't specify a size ...
        # We read the line in chunks to make sure it's not a 100MB line !
        res = []
        while True:
            data = self.rfile.readline(256)
            self.bytes_read += len(data)
            self._check_length()
            res.append(data)
            # See http://www.cherrypy.org/ticket/421
            if len(data) < 256 or data[-1:] == "\n":
                return ''.join(res)
    
    def readlines(self, sizehint = 0):
        # Shamelessly stolen from StringIO
        total = 0
        lines = []
        line = self.readline()
        while line:
            lines.append(line)
            total += len(line)
            if 0 < sizehint <= total:
                break
            line = self.readline()
        return lines
    
    def close(self):
        self.rfile.close()
    
    def __iter__(self):
        return self
    
    def next(self):
        data = self.rfile.next()
        self.bytes_read += len(data)
        self._check_length()
        return data


class Host(object):
    """An internet address.
    
    name should be the client's host name. If not available (because no DNS
        lookup is performed), the IP address should be used instead.
    """
    
    ip = "0.0.0.0"
    port = 80
    name = "unknown.tld"
    
    def __init__(self, ip, port, name=None):
        self.ip = ip
        self.port = port
        if name is None:
            name = ip
        self.name = name
    
    def __repr__(self):
        return "http.Host(%r, %r, %r)" % (self.ip, self.port, self.name)
