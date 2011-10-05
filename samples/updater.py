#!/usr/bin/env python
# encoding: utf-8

from playitapi.api import episodes_api, users_api, movies_api
from playitapi.batcher import QueuedBatchDocumentUpdater
from playitapi.utils import get_name, get_email, get_picture

import time
import sys
import json
from datetime import datetime

class FillUserData(QueuedBatchDocumentUpdater):

    api = users_api

    def process_doc(self, u):

        a, b = u.id.split('@')

        if u.get('social_data', None) is not None or u.get('social_auth', None) is not None:
            networks = dict()
            if b in ['facebook', 'mailru', 'vkontakte']:
                networks[b] = dict(data=u['social_data'], auth=u['social_auth'], status_updates=True)

            if b == 'twitter':
                networks[b] = dict(data=u['social_auth'], auth=u['social_data'], status_updates=True)

            u.social_networks = networks

            del u['social_auth']
            del u['social_data']
        else:
            if not u.social_networks:
                default = dict(data=None, auth=None, status_updates=True)
                u.social_networks = dict(
                    twitter = default,
                    facebook = default,
                    mailru = default,
                    vkontakte = default
                )

        u.name = get_name(u)
        u.email = get_email(u)
        u.picture = get_picture(u)

        return u

if __name__ == '__main__':

    FillUserData(map(lambda x: {"id" : x}, json.loads(sys.argv[1]))).run()




