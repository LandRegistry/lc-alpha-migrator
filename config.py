import os


class Config(object):
    DEBUG = False
    APPLICATION_NAME = "lc-migrator"


class DevelopmentConfig(Config):
    DEBUG = True
    B2B_LEGACY_URL = "http://10.0.2.2:15007"
    BANKRUPTCY_DATABASE_API = "http://localhost:5004"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"


class PreviewConfig(Config):
    B2B_LEGACY_URL = "http://localhost:5007"
    BANKRUPTCY_DATABASE_API = "http://localhost:5004"
    MQ_USERNAME = "mquser"
    MQ_PASSWORD = "mqpassword"
    MQ_HOSTNAME = "localhost"
    MQ_PORT = "5672"
