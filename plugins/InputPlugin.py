import logging
from plugins import plugin_interface as pi

class InputPlugin(pi.PluginInterface):
    CONFIG_ENABLE="enable"
    
    callback=None
    enable=True
    to_stop=False

    def __init__(self):
        super(InputPlugin, self).__init__()
        self.logger = logging.getLogger('InputPluginBase')
        self.plugin_type="input"
        self.to_stop = False

    def do_analytic(self):
        super(InputPlugin, self).do_analytic()
        while self.to_stop==False:
            self.load_data()
        self.logger.debug("Analytic loop is end")


    def set_callback(self, callback):
        self.callback=callback

    def stop(self):
        self.logger.debug("Plugin: stop")
        self.to_stop=True

    def load_data(self):
        pass