# -*- coding: utf-8 -*-

SORT_ASC = "ASC"
SORT_DESC = "DESC"

WHERE_GREATER = '>'
WHERE_GREATER_EQUAL = '>='
WHERE_EQUAL = '='
WHERE_LESS = '<'
WHERE_LESS_EQUAL = '<='
WHERE_NOT = '<>'
WHERE_BETWEEN = 'between'
WHERE_IN = 'in'
WHERE_NOT_IN = 'not in'
WHERE_CONTAIN = 'contain'  # https://www.postgresql.org/docs/9.3/static/functions-range.html
WHERE_NOT_CONTAIN = 'not contain'
WHERE_OVERLAP = "overlap"
WHERE_NOT_OVERLAP = "not overlap"
WHERE_OR = 'or'
WHERE_LIKE = 'like'


class Null:
    """Used in `~revopy.ds` to make difference with

    + No such field exists: Null() (no record)
    + Field contains nothing: None (null field in database table)
    """
    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        mod = self.__class__.__module__
        return '<{}.{} {:#x}>'.format(mod, self.__class__.__name__, id(self))


class Placeholder:
    """When a Placeholder is used in a bind context,
    it will not used as string placeholder.

    E.x: Not Placeholder: (see insert())
    INSERT INTO a_table (not_function)
    VALUES (%(not_function)s)

    Placeholder:
    INSERT INTO a_table (not_function)
    VALUES (USER_DEFINED_CALL(%(not_function)s))
    """
    def __init__(self, placeholder: str, bind_values: dict=None):
        """
        :param str placeholder:
               E.x "ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)"
        :param dict bind_values:
               A dict
        """
        self.placeholder = placeholder
        self.bind_values = bind_values or {}

    def __repr__(self):
        mod = self.__class__.__module__
        return '<{}.{} {:#x}: {} - {}>'.format(
            mod, self.__class__.__name__, id(self), self.placeholder, self.bind_values
        )


def is_null(value):
    return isinstance(value, Null)


def is_placeholder(value):
    return isinstance(value, Placeholder)

