import logging
from plugins import plugin_interface as pi
from core import globals
from importlib import import_module

class OutputPlugin(pi.PluginInterface):

    rules_configuration={}

    def __init__(self):
        super(OutputPlugin, self).__init__()
        self.logger = logging.getLogger('OutputPluginBase')
        self.plugin_type="output"

    def load_configuration(self):
        self.rules_configuration=self.return_configuration()
        if self.rules_configuration!=None:
            self.logger.debug("Configuration loaded: %s", self.rules_configuration)
            default=self.rules_configuration[globals.configuration.CONFIG_COLLECTION_FIELD_DATA_DEFAULT]
            for key, value in default.items():
                setattr(self, key, value)
        else:
            self.logger.debug("Configuration not found")
            self.init_configuration()

  
    def execute(self, alert): pass

    @staticmethod
    def load_plugin(plugin, rule):
        plugin_module=globals.outputs[plugin]
        module = import_module(plugin_module)
        cls = getattr(module, plugin)
        plugin=cls()
        plugin.do_master()
        if rule in plugin.rules_configuration:
            configuration=plugin.rules_configuration[rule]
            for key, value in configuration.items():
                setattr(plugin, key, value)
        return plugin


