# -*- coding: UTF-8 -*-

DEFAULT_FETCH_SIZE = 4096

class Operation(object):
    """
    Represents an operation to be executed on driver.
    """

    OPER_INSERT = 1
    OPER_UPDATE = 2

    oper_type = None
    data = None

    def __init__(self, oper_type, data):
        self.oper_type = oper_type
        self.data = data

    def as_insert(self):
        """
        Returns this same operation, but casted to insert.
        """

        return Operation(self.OPER_INSERT, self.data)

    def as_update(self):
        """
        Returns this same operation, but casted to update.
        """

        return Operation(self.OPER_UPDATE, self.data)

class BaseDriver(object):
    """
    Base classe for drivers.
    """

    def get_queryset(self):
        """
        Returns the queryset for all records.
        """

        raise NotImplementedError()

    def get_record(self, id):
        """
        Returns a record identified by it's id.
        """

        raise NotImplementedError()

    def execute_operation(self, operation):
        """
        Executes the operation specified.
        """

        raise NotImplementedError()

class BaseQuerySet(object):
    """
    Base class for implementing driver's querysets. This allow us to read
    tons of rows without consuming much memory, as result is paginated.
    """

    __data = None

    def __init__(self, batch_size=DEFAULT_FETCH_SIZE):
        pass

    def __iter__(self):
        return self

    def next(self):
        return self._fetch_row()

    def _fetch_row(self):
        """
        This is where the main driver logic for fetching rows is implemented.
        """

        raise NotImplementedError()
