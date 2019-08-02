HttpRanger
==========

File like python class, that reads remote file over HTTP using Range requests.
Use case is slow parsing of large files:
  * No need to store file on local storage and waste space and/or i/o requests of local storage device.
  * No need of keeping long-lived HTTP requests - web servers close connection if client does not read data fast enough because of various "idle" timeouts.
  * Seek is supported (although seeking is slow because we cache only single chunk of data, so seek may destroy the cache). This is needed for python2's `gzip` library.
