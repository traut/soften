#!/usr/bin/env python
# -*- coding: utf-8 -*-

from soften import couch
from soften.samples.docs import Movie

movies_api = couch.CouchAPI(
    server = 'localhost',
    database = 'movies',
    login = 'user-login',
    password = 'user-password',
    doc_class = Movie,
    design_docname = 'default'
)
