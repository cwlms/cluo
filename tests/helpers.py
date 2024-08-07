from cluo.core import Batch, Record


def dict_from_record(record, ignored_fields=[]):
    retval = {}
    for key, value in record.data.items():
        if key in ignored_fields:
            value = ""
        retval[key] = value
    return retval


def dicts_from_batch(batch, ignored_fields=[]):
    return [dict_from_record(record, ignored_fields) for record in batch.records]


def batch_from_dicts(rec_dicts: list[dict]) -> Batch:
    return Batch(records=[Record(data=rd) for rd in rec_dicts])
