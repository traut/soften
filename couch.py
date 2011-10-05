#!/usr/bin/env python
# encoding: utf-8

from couchdb import client
from couchdb import mapping
from couchdb.mapping import *
from couchdb.http import ResourceNotFound
from couchdb.design import ViewDefinition

class CustomDocument(mapping.Document):
    db = None

    def validate(self):
        pass

    def save(self, db=None):
        if not db:
            db = self.db
        if not db:
            raise ValueError(u"No database to save to")
        return self.store(db)


class CouchAPIError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


class CouchAPI(object):
    def __init__(self, server='http://127.0.0.1:5984', \
            login=None, password=None, database=None, design_docname=None, doc_class=CustomDocument()):
        self.design_docname = design_docname
        self.server = client.Server(server)
        self.doc_class = doc_class

        if login and password:
            self.server.resource.credentials = (login, password)

        try:
            db = self.server[database]
        except ResourceNotFound:
            db = self.server.create(database)

        self.db = db

    def create(self, **kwargs):
        doc = self.doc_class(**kwargs)
        doc.db = self.db
        return doc

    def get(self, doc_id):
        if doc_id:
            doc = self.doc_class.load(self.db, doc_id)
            if doc:
                doc.db = self.db
                return doc

    def __view(self, view, with_key_value=False, include_docs=True, **args):
        view_iter = self.db.view('%s' % view, include_docs=include_docs, **args)

        count = 0
        for r in view_iter.rows:
            count += 1
            if r.doc:
                if self.doc_class:
                    doc = self.doc_class.wrap(r.doc)
                    doc.db = self.db
                else:
                    doc = r.doc

                if with_key_value:
                    yield r.key, r.value, doc
                else:
                    yield doc
            else:
                if include_docs: # there should be a doc. if it's not there then we'll skip it cause it's a mistake or deleted content
                    continue

                yield r.key, r.value, r.id

    def all_docs_view(self, include_docs=True, **args):
        return self.__view('_all_docs', include_docs=include_docs, **args)

    def get_from_view(self, view, with_key_value=False, include_docs=True, design_docname=None, **args):

        if design_docname:
            view_full = '%s/%s' % (design_docname, view)
        elif self.design_docname:
            view_full = '%s/%s' % (self.design_docname, view)
        else:
            view_full = view
        return self.__view(view_full, with_key_value=with_key_value, include_docs=include_docs, **args)

    def all(self):
        for r in self.db.view('_all_docs', include_docs=True):
            if r.doc and r.doc.id != '_design/' + self.design_docname:
                if self.doc_class:
                    doc = self.doc_class.wrap(r.doc)
                    doc.db = self.db
                else:
                    doc = r.doc
                yield doc

    def sync_views(self, *views):
        print "Syncing views", views
        ViewDefinition.sync_many(self.db, views)

    def save(self, doc):
        return self.db.save(doc)

