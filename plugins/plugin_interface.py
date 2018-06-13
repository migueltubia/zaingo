import logging
from  core import globals

class PluginInterface(object):
    name = ""
    description = ""
    plugin_type = ""
    node = None

    def __init__(self):
        self.logger = logging.getLogger('PluginInterface')
        self.node = None

    def do_master(self):
        self.logger.debug("DO %s as master", self.name)
        self.load_configuration()
        self.initialize_master()

    def initialize_master(self):
        self.logger.debug("IN")
        p = {}
        p["name"] = self.name
        p["type"] = self.plugin_type
        node = globals.db.SearchDocuments(globals.engines_collection_object, p)

        if node == None or node.count() == 0:
            self.node = globals.db.CreateDocument(globals.engines_collection_object, p)
        else:
            self.node = node.next()

        if self.node != None:
            self.logger.debug("Node created")
        else:
            self.logger.error("Node not created")

        self.create_tasks()
        self.logger.debug("OUT")

    def create_tasks(self):
        pass

    def load_task(self, task):
        self.logger.debug("Loading task %s", task.name)
        task.create_task()

    def do_analytic(self):
        self.logger.debug("DO %s as analytic", self.name)
        self.load_configuration()
        self.initialize_analytic()

    def initialize_analytic(self):
        pass

    def init_configuration(self):
        configuration={}
        configuration[globals.configuration.CONFIG_COLLECTION_FIELD_DATA]={}
        configuration[globals.configuration.CONFIG_COLLECTION_FIELD_DATA][globals.configuration.CONFIG_COLLECTION_FIELD_DATA_DEFAULT] = self.create_configuration()
        globals.configuration.save_configuration(configuration=configuration, plugin_type=self.plugin_type, plugin_name=self.name)

    def create_configuration(self):
        return {}

    def return_configuration(self):
        configuration=globals.configuration.load_configuration(plugin_type=self.plugin_type, plugin_name=self.name, config_type=globals.configuration.CONFIG_COLLECTION_FIELD_DATA)
        return configuration

    def load_configuration(self):
        configuration=self.return_configuration()
        if configuration!=None:
            self.logger.debug("Configuration loaded: %s", configuration)
            default=configuration[globals.configuration.CONFIG_COLLECTION_FIELD_DATA_DEFAULT]
            if default!={}:
                for key, value in default.items():
                    setattr(self, key, value)
            if globals.configuration.name in configuration:
                node_config=configuration[globals.configuration.name]
                for key, value in node_config.items():
                    setattr(self, key, value)
            self.logger.debug("Plugin configured")
        else:
            self.logger.debug("Configuration not found")
            self.init_configuration()