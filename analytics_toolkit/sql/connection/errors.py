class SqlUtilsError(Exception):
    pass


class UnsupportedConnectionTypeError(SqlUtilsError):
    pass


class InvalidSqlInputError(SqlUtilsError):
    pass


class SqlConfigError(SqlUtilsError):
    pass
