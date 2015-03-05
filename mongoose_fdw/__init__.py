###
### Author: David Wallin
### Time-stamp: <2015-03-05 21:54:14 dwa>

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log2pg

from pymongo import MongoClient
from dateutil.parser import parse

from functools import partial

dict_traverser = partial(reduce,
                         lambda x, y: x.get(y, {}) if type(x) == dict else x)


def coltype_formatter(coltype):
    if coltype in ('timestamp without time zone', 'timestamp with time zone', 'date'):
        return lambda x: x if hasattr(x, 'isoformat') else parse(x)
    else:
        return None


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

        # log2pg('cols: {}'.format(columns))
        self.fields = {col: {'formatter': coltype_formatter(coldef.type_name),
                             'path': col.split('.')} for (col, coldef) in columns.items()}

    def build_spec(self, quals):
        Q = {}

        comp_mapper = {'>': '$gt',
                       '>=': '$gte',
                       '<=': '$lte',
                       '<': '$lt'}

        for qual in quals:
            val_formatter = self.fields[qual.field_name]['formatter']
            vform = lambda val: val_formatter(val) if val_formatter is not None else val
            if qual.operator == '=':
                Q[qual.field_name] = vform(qual.value)

            elif qual.operator in ('>', '>=', '<=', '<'):
                comp = Q.setdefault(qual.field_name, {})
                comp[comp_mapper[qual.operator]] = vform(qual.value)
                Q[qual.field_name] = comp

            else:
                log2pg('Qual operator {} not implemented yet: {}'.format(qual.field_name, qual))
        return Q

    def execute(self, quals, columns):

        ## Only request fields of interest:
        fields = {k: True for k in columns}
        if '_id' not in fields:
            fields['_id'] = False

        Q = self.build_spec(quals)
        # log2pg('spec: {}'.format(Q))
        # log2pg('fields: {}'.format(fields))
        cur = self.coll.find(spec=Q, fields=fields, snapshot=True)
        for doc in cur:
            yield {col: dict_traverser(self.fields[col]['path'], doc) for col in columns}


## Local Variables: ***
## mode:python ***
## coding: utf-8 ***
## End: ***
