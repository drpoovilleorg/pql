from .aggregation import AggregationGroupParser, AggregationParser
from .matching import (SchemaFreeParser, SchemaAwareParser, ParseError,
                       StringField, IntField, BoolField, IdField,
                       ListField, DictField, DateTimeField)

def find(expression, schema=None):
    parser = SchemaFreeParser() if schema is None else SchemaAwareParser(schema)
    return parser.parse(expression)

def _parse_dict(parser, dct):
    return dict([(k, parser.parse(v)) for k, v in dct.items()])
    
def group(_id, **kwargs):
    return {'$group': _parse_dict(parser=AggregationGroupParser(),
                                  dct=dict(kwargs, _id=_id))}

def project(**kwargs):
    return {'$project': _parse_dict(parser=AggregationParser(), dct=kwargs)}

def match(expression, schema=None):
    return {'$match': find(expression, schema)}

def limit(n):
    return {'$limit': n}

def skip(n):
    return {'$skip': n}

def unwind(l):
    return {'$unwind': '$' + l}

def sort(fields):
    from pymongo import ASCENDING, DESCENDING
    from bson import SON
    
    sort = []
    for field in fields:
        if field.startswith('-'):
            field = field[1:]
            sort.append((field, DESCENDING))
            continue
        elif field.startswith('+'):
            field = field[1:]
        sort.append((field, ASCENDING))
    return {'$sort': SON(sort)}