class Null(object):
    """Used in `~revopy.ds` to make difference with

    + No such field exists: Null() (no record)
    + Field contains nothing: None (null field in database table)
    """
    pass


class Placeholder(object):
    """When a Placeholder is used in a binded context,
    it will not used as string placeholder.

    E.x: Not Placeholder: (see insert())
    INSERT INTO a_table (not_function)
    VALUES (%(not_function)s)

    Placeholder:
    INSERT INTO a_table (not_function)
    VALUES (USER_DEFINED_CALL(%(not_function)s))
    """

    def __init__(self, placeholder, binded_values=None):
        """
        :param placeholder: str
               E.x "ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)"
        :param binded_values: dict
               A dict
        """
        self.placeholder = placeholder
        self.binded_values = binded_values or {}


def is_null(value):
    return isinstance(value, Null)


def is_placeholder(value):
    return isinstance(value, Placeholder)

