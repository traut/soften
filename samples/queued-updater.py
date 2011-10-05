#!/usr/bin/env python
# encoding: utf-8

from soften.samples.api import movies_api
from soften.samples.batcher import QueuedBatchDocumentUpdater

import time
import sys
import json
from datetime import datetime

class FillMovieData(QueuedBatchDocumentUpdater):

    api = movies_api

    def process_doc(self, m):
        stamp = time.time()
        m.title = dict(
            ru = 'new title in russian %s' % stamp,
            en = 'new title in english %s' % stamp
        )
        return m

if __name__ == '__main__':
    FillUserData(json.loads(sys.argv[1])).run()




