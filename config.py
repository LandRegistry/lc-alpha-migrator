import os


class Config(object):
    APPLICATION_NAME = "lc-migrator"
    DEBUG = os.getenv('DEBUG', True)
    SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI", "postgresql://landcharges:lcalpha@localhost/landcharges")
    AMQP_URI = os.getenv("AMQP_URI", "amqp://mquser:mqpassword@localhost:5672")
    PSQL_CONNECTION = os.getenv("PSQL_CONNECTION", "dbname='landcharges' user='landcharges' host='localhost' password='lcalpha'")

    LEGACY_ADAPTER_URI = os.getenv('LEGACY_ADAPTER_URL', 'http://10.0.2.2:15007')
    #LEGACY_ADAPTER_URI = os.getenv('LEGACY_ADAPTER_URL', 'http://localhost:5007')
    LAND_CHARGES_URI = os.getenv('LAND_CHARGES_URL', 'http://localhost:5004')
