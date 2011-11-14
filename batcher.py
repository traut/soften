#!/usr/bin/env python
# encoding: utf-8

import json

from multiprocessing import Pool

from couchdb.mapping import *

from soften.couch import CouchAPIError

import logging
logging.basicConfig()

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    if n:
        for i in xrange(0, len(l), n):
            yield l[i : i+n]


class BatchDocumentUpdater(object):

    rows_processed = 0

    batch_size = 1000

    api = None

    loglevel = logging.DEBUG

    starts_from_batch = 0

    def __init__(self, api=None):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(self.loglevel)
        if api:
            self.api = api

        self.log.info("Batcher created")

    def process_doc(self, doc):
        raise NotImplementedError()

    def get_all_ids(self):
        self.log.info("Getting _all_docs")
        return list(self.api.db.view('_all_docs'))

    def get_chunk_docs(self, chunk):
        return self.api.db.view('_all_docs', keys=[c['id'] for c in chunk], include_docs=True)

    def process_chunk(self, chunk_pair):
        counter, chunk = chunk_pair

        self.log.info("Processing chunk %d..." % (counter + 1))

        rows = self.get_chunk_docs(chunk)

        rows_updated = []

        for row in rows:
            try:
                doc = self.api.doc_class.wrap(row.doc)

                doc = self.process_doc(doc)

                if not doc:
                    continue

                try:
                    doc.validate()
                except CouchAPIError, e:
                    self.log.error(u"%s doesn't validate, skipping:" % doc.id, exc_info=True)
                    continue

                rows_updated.append(doc)
                self.rows_processed += 1
            except Exception, e:
                self.log.error("Exception with row: %s" % row, exc_info=True)

        if rows_updated:
            committed_ok = 0
            committed_not_ok = []
            for status, id, rev in self.api.db.update(rows_updated):
                if status:
                    committed_ok += 1
                else:
                    committed_not_ok.append(id)

            self.log.info("OK: %d, Failed: %s" % (committed_ok, committed_not_ok))
            self.log.info("Batch %d with %d elements committed" % ((counter + 1), committed_ok))
        else:
            self.log.info("OK: No rows updated")

    def run(self):
        if not self.api:
            raise Exception("CouchAPI instance should be specified as Batcher field")

        self.log.info(u"Batcher started")
        for n, chunk in enumerate(chunks(self.get_all_ids(), self.batch_size)):
            if not self.starts_from_batch or n >= self.starts_from_batch:
                self.process_chunk((n, chunk))
        self.log.info(u"Batcher finished")



class ConcurrentBatchDocumentUpdater(BatchDocumentUpdater):

    PROCESSES = 4

    def run(self):
        if not self.api:
            raise Exception("CouchAPI instance should be specified as Batcher field")

        self.log.info(u"Batcher started")
        pool = Pool(self.PROCESSES)

        chs = chunks(self.get_all_ids(), self.batch_size)
        chs = filter(lambda x: not self.starts_from_batch or x[0] >= self.starts_from_batch, enumerate(chs))
        if self.starts_from_batch:
            self.log.info("Starting from batch %s" % self.starts_from_batch)

        pool.map(self.process_chunk, chs, chunksize=1)
        self.log.info(u"Batcher finished")

try:
    import beanstalkc

except ImportError:
    logging.error("Can't import beanstalkc. soften.batcher.BatchToQueuePusher will not work")


class BatchToQueuePusher(object):

    def __init__(self, queue_name, server_name, server_port, api=None, starts_from=0, logging_level=None, chunk_size=100):

        self.api = api
        self.starts_from_batch = starts_from
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(logging_level or logging.DEBUG)

        self.starts_from = starts_from
        self.chunk_size = chunk_size

        self.conn = beanstalkc.Connection(server_name, server_port)
        self.conn.use(queue_name)


        self.log.info("Queue pusher created")

    def get_all_ids(self):
        self.log.info("Getting _all_docs")
        return list(self.api.db.view('_all_docs'))

    def run(self):
        ids = [x['id'] for x in self.get_all_ids()[self.starts_from:]]

        for n, c in enumerate(chunks(ids, self.chunk_size)):
            self.log.info("Pushing chunk %d" % n)
            self.conn.put(json.dumps(c), ttr=1000000)

        self.log.info("Done")

class QueuedBatchDocumentUpdater(BatchDocumentUpdater):

    def __init__(self, ids_list):
        self.ids = map(lambda x: {"id" : x}, ids_list)
        self.batch_size = len(self.ids)

        super(QueuedBatchDocumentUpdater, self).__init__()

    def get_all_ids(self):
        return list(self.api.db.view('_all_docs', keys=[x['id'] for x in self.ids]))


