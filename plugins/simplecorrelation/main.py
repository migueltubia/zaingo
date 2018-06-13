import logging
import datetime

from plugins import analysis_interface as ai
from core import globals
from . import control
from . import rule
from . import tasks

class SimpleCorrelation(ai.EngineInterface):

    rules = []
    objects = {}

    #Colecciones de configuración
    CONFIG_COLLECTION="simplecorrelation_configuration_collection"
    
    #Colecciones para el análisis
    RULE_COLLECTION = "simplecorrelation_rule"
    CONTROL_COLLECTION = "simplecorrelation_control"
    GRAPH = "simplecorrelation_graph"
    EDGES = "simplecorrelation_edges"

    #Tipos de relaciones
    RELATION_RULE_CONTROL = "RULE_CONTROL"
    RELATION_CONTROL_CONTROL = "CONTROL_CONTROL"

    #Tipos de relación en la alerta
    ALERT_TYPE_CONTROL = "CONTROL"


    def __init__(self):
        super(SimpleCorrelation, self).__init__()
        self.name = "SimpleCorrelation"
        self.logger = logging.getLogger(self.name)
        self.description = "Engine for simple correlation based analysis"
        self.rules = []

    def initialize_master(self):
        super(SimpleCorrelation, self).initialize_master()
        globals.db.CreateCollection(SimpleCorrelation.CONFIG_COLLECTION, False, truncate=False)

        globals.db.CreateCollection(SimpleCorrelation.RULE_COLLECTION, False, truncate=False)
        globals.db.CreateCollection(SimpleCorrelation.CONTROL_COLLECTION, False, truncate = False)
        globals.db.CreateCollection(SimpleCorrelation.EDGES, True, truncate = False)
        globals.db.DefineGraph(SimpleCorrelation.GRAPH,
                               [SimpleCorrelation.CONTROL_COLLECTION, SimpleCorrelation.RULE_COLLECTION],
                               [SimpleCorrelation.CONTROL_COLLECTION, SimpleCorrelation.RULE_COLLECTION],
                               [SimpleCorrelation.CONTROL_COLLECTION], SimpleCorrelation.EDGES)

    def create_configuration(self):
        configuration = {}
        return configuration

    def create_tasks(self):
        pass

    def initialize_analytic(self):
        self.rules = SimpleCorrelationConfiguration.load_configuration()

    def execute(self, data):
        self.logger.debug("Received new data on engine")
        control_time = datetime.datetime.now().timestamp()
        used_in = []
        for r in self.rules:
            self.logger.debug("Begin analysis")
            data.control_time = control_time
            nodes = r.analyze_event(data)
            used_in += nodes
        self.logger.debug("End of analysis")

        engine_data = {}
        engine_data["used"] = False

        if len(used_in) > 0:
            engine_data["used"] = True
            engine_data["data"] = []

            for node in used_in:
                engine_data["data"].append(node["_id"])

        return engine_data

    def generate_alert(self, parameters):
        node_id = parameters["alert"]
        alert = None

        alert_data = None
        try:
            ds_temp = globals.db.get_collection(SimpleCorrelation.SCENARIO_COLLECTION).get(node_id)
            if ds_temp != None:

                alert_data = control.ControlData()
                alert_data.from_json(ds_temp)
        except Exception as error:
                self.logger.error("Error creating Alert object: %s", error)
        return alert_data

class SimpleCorrelationConfiguration:

    logger = logging.getLogger("SimpleCorrelationConfiguration")

    @staticmethod
    def load_configuration():
        rules = []
        all = globals.db.SearchDocuments(SimpleCorrelation.CONFIG_COLLECTION)
        nodos = 0
        nodos = all.count()
        computed = 0
        while computed < nodos:
            computed += 1
            s = all.next()
            rule_init =  rule.Rule.create_rule(s)
            rules.append(rule_init)
            SimpleCorrelation.objects[s["_id"]] = rule_init

        return rules