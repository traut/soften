import time

from soften import couch

from couchdb.mapping import *

class Movie(couch.CustomDocument):

    title = DictField(Mapping.build(
        en = TextField(default=None),
        ru = TextField(default=None)
    ))

    mp4 = DictField(Mapping.build(
        en = DictField(Mapping.build(
            p720 = TextField(default=None),
            p480 = TextField(default=None),
        )),
        ru = DictField(Mapping.build(
            p720 = TextField(default=None),
            p480 = TextField(default=None),
        )),
    ))

    year = IntegerField()

    created = IntegerField(default=lambda: int(time.time()))

    changed = IntegerField()

    def save(self):
        if not self.changed:
            self.changed = int(time.time())
        return super(Movie, self).save()




