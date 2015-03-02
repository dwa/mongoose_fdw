###
### Author: David Wallin
### Time-stamp: <2015-03-02 08:56:11 dwa>

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log2pg

from pymongo import MongoClient


class Mongoose_fdw (ForeignDataWrapper):

    def __init__(self, options, columns):
        super(Mongoose_fdw, self).__init__(options, columns)

        self.host_name = options.get('host', 'localhost')
        self.port_nr = int(options.get('port', '27017'))

        self.user = options.get('user')
        self.password = options.get('password')

        self.db_name = options.get('db', 'test')
        self.collection_name = options.get('collection', 'test')

        self.c = MongoClient(host=self.host_name,
                             port=self.port_nr)

        self.auth_db = options.get('auth_db', self.db_name)

        self.c.userprofile.authenticate(self.user,
                                        self.password,
                                        source=self.auth_db)

        self.db = getattr(self.c, self.db_name)
        self.coll = getattr(self.db, self.collection_name)

    def execute(self, quals, columns):
        ## TODO: build spec based on quals:
        if quals:
            log2pg('quals: {}'.format(quals))
            log2pg('Quals are not implemented yet')

        ## Only request fields of interest:
        fields = {k: True for k in columns.keys()}
        if '_id' not in fields:
            fields['_id'] = False

        Q = {}
        cur = self.coll.find(spec=Q, fields=fields, snapshot=True)
        for doc in cur:
            yield doc

## Local Variables: ***
## mode:python ***
## coding: utf-8 ***
## End: ***
