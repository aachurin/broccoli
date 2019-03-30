import os
import importlib
import importlib.util
from broccoli.types import App, Config
from broccoli.components import Component

__all__ = ('ConfigComponent',)


class ConfigComponent(Component):
    singleton = True

    def __init__(self,
                 module_environ_var='BROCCOLI_CONFIG_MODULE',
                 settings_module='settings',
                 defaults=None):
        self.module_environ_var = module_environ_var
        self.settings_module = settings_module
        self.defaults = defaults

    def resolve(self, app: App) -> Config:
        module = os.environ.get(self.module_environ_var, self.settings_module)
        ret = dict(app.settings) if app.settings else {}
        if importlib.util.find_spec(module):
            conf = importlib.import_module(module)
            if self.defaults:
                ret.update(self.defaults)
            for key in dir(conf):
                if key.startswith('_'):
                    continue
                ret[key] = getattr(conf, key)
        return ret


CONFIG_COMPONENTS = [
    ConfigComponent()
]
