from .base import MySQLDialect


class MariaDBDialect(MySQLDialect):
    is_mariadb = True


def loader(driver):
    driver_mod = __import__(
        "sqlalchemy.dialects.mysql.%s" % driver
    ).dialects.mysql
    driver_cls = getattr(driver_mod, driver).dialect

    return type(
        "MariaDBDialect_%s" % driver, (MariaDBDialect, driver_cls,), {}
    )
