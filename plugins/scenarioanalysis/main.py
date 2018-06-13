import logging
import datetime

from plugins import analysis_interface as ai
from core import globals
from . import scenario
from . import level
from . import control
from . import condition
from . import tasks

class ScenarioAnalysis(ai.EngineInterface):

    scenarios = []
    objects = {}

    #Colecciones de configuración
    SCENARIO_CONFIG_COLLECTION = "scenario_configuration_collection"
    LEVEL_CONFIG_COLLECTION = "level_configuration_collection"
    CONTROL_CONFIG_COLLECTION = "control_configuration_collection"
    CONDITION_CONFIG_COLLECTION = "condition_configuration_collection"
    SCENARIO_CONFIG_GRAPH = "scenario_configuration_graph"
    SCENARIO_CONFIG_EDGES = "scenario_configuration_edges"
    
    #Colecciones para el análisis
    SCENARIO_COLLECTION = "scenario_collection"
    LEVEL_COLLECTION = "level_collection"
    CONTROL_COLLECTION = "control_collection"
    SCENARIO_GRAPH = "scenario_graph"
    SCENARIO_EDGES = "scenario_edges"

    #Tipos de relaciones
    RELATION_SCENARIO_LEVEL = "SCENARIO_LEVEL"
    RELATION_LEVEL_CONTROL = "LEVEL_CONTROL"
    RELATION_CONTROL_CONTROL = "CONTROL_CONTROL"
    RELATION_LEVEL_LEVEL = "LEVEL_LEVEL"
    RELATION_CONTROL_CONDITION = "CONTROL_CONDITION"

    #Tipos de relación en la alerta
    ALERT_TYPE_LEVEL = "LEVEL"
    ALERT_TYPE_CONTROL = "CONTROL"


    def __init__(self):
        super(ScenarioAnalysis, self).__init__()
        self.name = "ScenarioAnalysis"
        self.logger = logging.getLogger(self.name)
        self.description = "Engine for scenario based analysis"
        self.scenarios = []

    def initialize_master(self):
        super(ScenarioAnalysis, self).initialize_master()
        globals.db.CreateCollection(ScenarioAnalysis.SCENARIO_CONFIG_COLLECTION, False, truncate=False)
        globals.db.CreateCollection(ScenarioAnalysis.LEVEL_CONFIG_COLLECTION, False, truncate=False)
        globals.db.CreateCollection(ScenarioAnalysis.CONTROL_CONFIG_COLLECTION, False, truncate=False)
        globals.db.CreateCollection(ScenarioAnalysis.CONDITION_CONFIG_COLLECTION, False, truncate=False)
        globals.db.CreateCollection(ScenarioAnalysis.SCENARIO_CONFIG_EDGES, True, truncate=False)
        globals.db.DefineGraph(ScenarioAnalysis.SCENARIO_CONFIG_GRAPH,
                               [ScenarioAnalysis.SCENARIO_CONFIG_COLLECTION,
                                ScenarioAnalysis.LEVEL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONTROL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONDITION_CONFIG_COLLECTION],
                               [ScenarioAnalysis.SCENARIO_CONFIG_COLLECTION,
                                ScenarioAnalysis.LEVEL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONTROL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONDITION_CONFIG_COLLECTION],
                               [ScenarioAnalysis.LEVEL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONTROL_CONFIG_COLLECTION,
                                ScenarioAnalysis.CONDITION_CONFIG_COLLECTION],
                               ScenarioAnalysis.SCENARIO_CONFIG_EDGES)

        globals.db.CreateCollection(ScenarioAnalysis.SCENARIO_COLLECTION, False, truncate = False)
        globals.db.CreateCollection(ScenarioAnalysis.LEVEL_COLLECTION, False, truncate = False)
        globals.db.CreateCollection(ScenarioAnalysis.CONTROL_COLLECTION, False, truncate = False)
        globals.db.CreateCollection(ScenarioAnalysis.SCENARIO_EDGES, True, truncate = False)
        globals.db.DefineGraph(ScenarioAnalysis.SCENARIO_GRAPH,
                               [ScenarioAnalysis.SCENARIO_COLLECTION, ScenarioAnalysis.LEVEL_COLLECTION,
                                ScenarioAnalysis.CONTROL_COLLECTION],
                               [ScenarioAnalysis.SCENARIO_COLLECTION, ScenarioAnalysis.LEVEL_COLLECTION,
                                ScenarioAnalysis.CONTROL_COLLECTION],
                               [ScenarioAnalysis.LEVEL_COLLECTION,
                                ScenarioAnalysis.CONTROL_COLLECTION], ScenarioAnalysis.SCENARIO_EDGES)

    def create_configuration(self):
        configuration = {}
        return configuration

    def create_tasks(self):
        super(ScenarioAnalysis, self).load_task(tasks.MarkAsInactive())
        super(ScenarioAnalysis, self).load_task(tasks.CalculateThreat())
        super(ScenarioAnalysis, self).load_task(tasks.ManageScenarios())

    def initialize_analytic(self):
        self.scenarios = ScenarioConfiguration.load_configuration()

    def execute(self, data):
        self.logger.debug("IN: execute")
        control_time = datetime.datetime.now().timestamp()
        used_in = []
        for s in self.scenarios:
            self.logger.debug("Begin analysis")
            data.control_time = control_time
            nodes = s.analyze_event(data)
            used_in += nodes
        self.logger.debug("End of analysis")

        engine_data = {}
        engine_data["used"] = False

        if len(used_in) > 0:
            self.logger.debug("Used in %s nodes", used_in)
            engine_data["used"] = True
            engine_data["data"] = []

            for node in used_in:
                engine_data["data"].append(node["_id"])

        self.logger.debug("OUT: execute")

        return engine_data

    def generate_alert(self, parameters):
        node_id = parameters["alert"]
        alert = None

        alert_data = None
        try:
            ds_temp = globals.db.get_collection(ScenarioAnalysis.SCENARIO_COLLECTION).get(node_id)
            if ds_temp != None:
                #config_db = globals.db.GetDocument(ds_temp["internal_id"])
                #if config_db != None:
                #    scenario_config = scenario.Scenario.create_scenario ( config_db )

                alert_data = scenario.ScenarioData()
                #alert_data.config_object = scenario_config
                alert_data.from_json(ds_temp)
        except Exception as error:
                self.logger.error("Error creating Alert object: %s", error)
        return alert_data

class ScenarioConfiguration:

    logger = logging.getLogger("ScenarioConfiguration")

    @staticmethod
    def load_configuration():
        scenarios = []
        all = globals.db.SearchDocuments(ScenarioAnalysis.SCENARIO_CONFIG_COLLECTION)
        nodos = all.count()
        computed = 0
        while computed < nodos:
            computed += 1
            s = all.next()
            scenario_init = scenario.Scenario.create_scenario(s)
            scenario_full = None
            try:
                scenario_full = ScenarioConfiguration.load_scenario(scenario_init)
            except Exception as error:
                ScenarioConfiguration.logger.error("Error loading scenario configuration: %s", error)
            if scenario_full != None:
                scenarios.append(scenario_full)
            else:
                ScenarioConfiguration.logger.debug("No scenarios found")
        return scenarios

    @staticmethod
    def load_scenario(_scenario):
        #vertex_filters = "vertex.type === \"" + ScenarioAnalysis.RELATION_SCENARIO_LEVEL + "\""
        edges = []
        ScenarioAnalysis.objects[_scenario.internal_id]=_scenario
        vertex_filters = ""
        levels_paths = globals.db.Graph_Go_Forward(ScenarioAnalysis.SCENARIO_CONFIG_GRAPH, _scenario.internal_id, vertex_filters = vertex_filters)
        if levels_paths == None:
            ScenarioConfiguration.logger.warning("No transversal found")
            return None
        levels_paths = levels_paths["paths"]
        for p in levels_paths:
            for v in p["vertices"]:
                if v["_id"] not in ScenarioAnalysis.objects.keys():
                    if ScenarioAnalysis.LEVEL_CONFIG_COLLECTION in v["_id"]:
                        ScenarioConfiguration.logger.debug("Creating Level")
                        level_load = level.Level.create_level(v)
                        level_load.scenario = _scenario
                        ScenarioAnalysis.objects[v["_id"]] = level_load
                    elif ScenarioAnalysis.CONTROL_CONFIG_COLLECTION in v["_id"]:
                        ScenarioConfiguration.logger.debug("Creating control")
                        control_load = control.Control.create_control(v)
                        ScenarioAnalysis.objects[v["_id"]] = control_load
                    elif ScenarioAnalysis.CONDITION_CONFIG_COLLECTION in v["_id"]:
                        ScenarioConfiguration.logger.debug("Creating condition")
                        cond_load = condition.Condition.create_condition(v)
                        ScenarioAnalysis.objects[v["_id"]] = cond_load
            for e in p["edges"]:
                if e not in edges:
                    edges.append(e)
                    _from = ScenarioAnalysis.objects[e["_from"]]
                    _to = ScenarioAnalysis.objects[e["_to"]]
                    if e["type"] == ScenarioAnalysis.RELATION_SCENARIO_LEVEL:
                        ScenarioConfiguration.logger.debug("Creating relation from scenario to level")
                        _from.first_level = _to
                    elif e["type"] == ScenarioAnalysis.RELATION_LEVEL_CONTROL:
                        ScenarioConfiguration.logger.debug("Creating relation from level to control")
                        _to.level = _from
                        _from.controls.append (_to)
                    elif e["type"] == ScenarioAnalysis.RELATION_LEVEL_LEVEL:
                        ScenarioConfiguration.logger.debug("Creating relation from level to level")
                        _from.next_level = _to
                    elif e["type"] == ScenarioAnalysis.RELATION_CONTROL_CONDITION:
                        ScenarioConfiguration.logger.debug("Creating relation from control to condition")
                        _from.condition = _to
        return _scenario