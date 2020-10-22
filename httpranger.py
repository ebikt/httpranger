#!/usr/bin/python
CACHE_SIZE = 16*1024*1024

import io
import errno
import re

try:
    # Python3
    import urllib.request as urllib2
except ImportError:
    # Python2
    import urllib2


class Urllib2Adapter(object):
    """ HttpRanger adapter for underlying HTTP library """
    def __init__(self, url):
        self.url = url

    def head(self):
        """ Issues HEAD request to obtain 'Content-Length' and 'Accept-Ranges'.
            Asserts that return statuscode is 200.
            Returns case-insentivie dictionary of http headers of response. """
        q = urllib2.Request(self.url)
        q.get_method = lambda: "HEAD"
        r = urllib2.urlopen(q)
        assert r.getcode() == 200
        return r.info()

    def range(self, offset, length):
        """ Issues GET request with specified range (in bytes).
            Asserts that response statuscode is 206.
            Asserts that returned data has length equal to what is declared in 'Content-Length'.
            Returns fetched data (as bytes object)."""
        q = urllib2.Request(self.url, headers={'Range': 'bytes={start}-{end}'.format(start = offset, end= offset + length - 1)})
        r = urllib2.urlopen(q)
        assert r.getcode() == 206
        data = r.read()
        i = r.info()
        assert len(data) == int(i['Content-Length'])
        return data


class HttpRanger(io.BufferedIOBase):
    """ File like object providing methods:
        .read(count)
        .seek(offset, whence=0)
        .tell()

        Backed by cached access to remote large file accessible via HTTP GET requests with range.
    """

    def __init__(self, url, cache_size = CACHE_SIZE, adapter = None):
        if adapter is None:
            if hasattr(url, 'head') and hasattr(url, 'range'):
                # url object already has adapter interface, thus assign
                # identity to adapter factory function
                adapter = lambda x: x
            else:
                # default adapter
                adapter = Urllib2Adapter
        self.adapter = adapter(url)
        self.info = self.adapter.head()
        assert re.search(r'\bbytes\b', self.info['Accept-Ranges'])
        self.size = int(self.info['Content-Length'])
        self.ctype  = self.info['Content-Type']
        self.cache_size = cache_size
        self.buffer_position = 0
        self.buffer = b""
        self.position = 0

        self._newline = b'\n'

        # 'file' compatibility (python2 gzip uses this)
        self.mode   = 'rb'

    def _range(self, offset, length):
        data = self.adapter.range(offset, length)
        assert len(data) == min(length, self.size - offset)
        return data

    # {{{ IOBase
    def close(self):
        super(HttpRanger, self).close()
        self.buffer = None

    def fileno(self):
        raise IOError("Not a file descriptor backed object", errno=errno.EBADF)

    def readable(self):
        self._checkClosed()
        return True

    def seek(self, offset, whence = 0):
        offset += [0, self.position, self.size][whence]
        self.position = max(0, min(self.size, offset))
        return self.position

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def truncate(self):
        self._checkWritable()
        assert False

    def writable(self):
        return False

    def _peek(self, count):
        if self.position < self.buffer_position or self.position + count > self.buffer_position + len(self.buffer):
            self.buffer = self._range(self.position, self.cache_size)
            self.buffer_position = self.position
        return self.buffer[self.position - self.buffer_position:]

    def peek(self, count = 1):
        # Not part of IOBase, but default implementation of IOBase.readline()
        # utilizes this function for better performance.
        count = min(max(1, count), self.cache_size, self.size - self.position)
        return self._peek(count)
    # }}}


    # {{{ BufferedIOBase
    def read(self, count = -1):
        self._checkClosed()
        if count < 0:
            count = self.size - self.position
        else:
            count = min(count, self.size - self.position)
        if count <= 0 or self.position >= self.size:
            return b""
        if count > int(self.cache_size / 2):
            # Too large read, cache bypass is good enough
            data = self._range(self.position, count)
        else:
            data = self.peek(count)[:count]
            if self.position < self.buffer_position or self.position + count > self.buffer_position + len(self.buffer):
                self.buffer = self._range(self.position, self.cache_size)
                self.buffer_position = self.position
            buffer_offset = self.position - self.buffer_position
            data = self.buffer[buffer_offset:buffer_offset + count]
        self.position += len(data)
        return data

    def readall(self):
        return self.read()

    def detach(self):
        raise io.UnsupportedOperation("HttpRanger does not support detach().")

    def read1(self, n=-1):
        return self.read()
    # }}}

if __name__ == "__main__":
    import time, sys
    r = HttpRanger(sys.argv[1])
    sys.stdout.write("Size:{r.size} Content-Type:{r.ctype}\n".format(r = r))
    if "gzip" in r.ctype:
        import gzip
        f = gzip.GzipFile(fileobj=r)
    else:
        f = r
    nlines = 0
    t = time.time() + 0.2
    for line in f:
        nlines = nlines + 1
        if t < time.time():
            t = time.time() + 0.2
            sys.stdout.write("{nlines:,}\r".format(nlines = nlines))
            sys.stdout.flush()
    sys.stdout.write("Read {nlines:,} lines\n".format(nlines = nlines))
