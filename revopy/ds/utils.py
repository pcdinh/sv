# -*- coding: utf-8 -*-

from asyncpg import Record
from typing import Dict, List, Tuple, Union


def records_to_dict(record: Union[Record, List[Record]]) -> Union[Dict, List[Dict]]:
    """Convert a ``asyncpg.Record`` or a list of ``asyncpg.Record`` to a ``dict`` or a ``list`` of ``dict``

    :param Record|List[Record] record:
    :return:
    """
    if isinstance(record, Record):
        return dict(record)
    return [dict(row) for row in record]
