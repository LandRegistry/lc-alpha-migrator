import os


class Config(object):
    DEBUG = False
    APPLICATION_NAME = "lc-migrator"


class DevelopmentConfig(Config):
    DEBUG = True
    B2B_LEGACY_URL = "http://localhost:5007"
    BANKRUPTCY_DATABASE_API = "http://localhost:5004"


class PreviewConfig(Config):
    B2B_LEGACY_URL = "http://localhost:5007"
    BANKRUPTCY_DATABASE_API = "http://localhost:5004"
