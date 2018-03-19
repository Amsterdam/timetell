import urllib

from aiohttp import web

from . import config, handlers

_DEFAULT_BINDPORT = 8000
_DEFAULT_BINDADDR = '0.0.0.0'
_DEFAULT_BASEURL  = 'http://localhost:8000/'


class Application(web.Application):
    # language=rst
    """The Application.
    """

    def __init__(self, *args, config_path: str=None, **kwargs):
        # add required middlewares
        if 'middlewares' not in kwargs:
            kwargs['middlewares'] = []
        kwargs['middlewares'].extend([
            web.normalize_path_middleware(),  # todo: needed?
        ])
        super().__init__(*args, **kwargs)

        # Initialize config:
        self._config = config.load(config_path)

        # get path
        path = urllib.parse.urlparse(self.baseurl).path
        if len(path) == 0 or path[-1] != '/':
            path += '/'

        # set routes
        self.router.add_post(path + 'upload', handlers.upload.post)
        self.router.add_get(path + 'system/health', handlers.systemhealth.get)

    @property
    def config(self) -> config.ConfigDict:
        return self._config

    @property
    def bindport(self) -> int:
        if 'bindport' in self.config['web']:
            return self.config['web']['bindport']
        return _DEFAULT_BINDPORT

    @property
    def bindaddr(self) -> str:
        if 'bindhost' in self.config['web']:
            return self.config['web']['bindhost']
        return _DEFAULT_BINDADDR

    @property
    def baseurl(self) -> str:
        if 'baseurl' in self.config['web']:
            return self.config['web']['baseurl']
        return _DEFAULT_BASEURL
