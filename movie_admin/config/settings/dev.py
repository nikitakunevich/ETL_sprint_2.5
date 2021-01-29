# noinspection PyUnresolvedReferences
from config.settings.base import *

INSTALLED_APPS += ['debug_toolbar']

MIDDLEWARE = ['debug_toolbar.middleware.DebugToolbarMiddleware'] + MIDDLEWARE
