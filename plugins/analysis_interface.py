import logging
from core import globals
from plugins import plugin_interface as pi
import datetime
from core import alerts
import asyncio


class EngineInterface(pi.PluginInterface):
    node = None
    alert_type = "alert"

    def __init__(self):
        super(EngineInterface, self).__init__()
        self.logger = logging.getLogger('EngineInterface')
        self.plugin_type = "engine"

    def execute(self, data): 
        pass

    @asyncio.coroutine
    def execute_internal(self, data):
        self.save_event_engine(data)
        engine_data = self.execute(data)
        if engine_data != None:
            engine_data = {}
            engine_data["used"] = False

        if "used" not in engine_data.keys():
            engine_data["used"] = False

        self.use_data(data, engine_data)

    def new_alert(self, **kwargs):
        if globals.configuration.node_mode == globals.configuration.NODE_ANALYSIS:
            globals.analytic_thread.order_new_alert(self.name, kwargs)
        if globals.configuration.node_mode == globals.configuration.NODE_MASTER or globals.configuration.node_mode == globals.configuration.NODE_FULL:
            alert_data = self.generate_alert(kwargs)
            if alert_data != None and isinstance(alert_data, alerts.AlertableInterface):
                self.logger.debug("Creating alert %s", alert_data)
                try:
                    alert_data.execute()
                    self.logger.debug("Alert created OK %s")
                except Exception as error:
                    self.logger.error("Error creating Alert object: %s", error)
                    alert = None

    def generate_alert(self, parameters):
        return None

    def save_event_engine(self, data):
        data_edge = {}
        data_edge["data"] = {}
        data_edge["timestamp"] = datetime.datetime.now().timestamp()
        data_edge["used"] = False
        globals.db.CreateEdge(globals.events_graph_object, data._id, self.node["_id"],
                              globals.events_edges, data_edge, "USED_IN")

    '''
    Se espera engine_data con formato json y campos:
    - used: boolean. Indicando si se considera que el evento está usando o no
    - data: object (opcional), indicando datos adicionales, propios del engine.
    '''
    def use_data(self, data, engine_data):
        for _e in globals.events_edges.find({"_from": data._id, "_to": self.node["_id"]}):
            _e["data"] = engine_data["data"]
            _e["used"] = engine_data["used"]
            globals.events_edges.update(_e)

    def get_data(self, node_id):
        query = ""