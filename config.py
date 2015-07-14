import os


class Config(object):
    DEBUG = False


class DevelopmentConfig(object):
    DEBUG = True
    B2B_LEGACY_URL = "http://localhost:5007"
    BANKRUPTCY_DATABASE_API = "http://localhost:5004"
