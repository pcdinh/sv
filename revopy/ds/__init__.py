# -*- coding: utf-8 -*-


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

