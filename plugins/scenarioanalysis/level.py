from . import base_object as bo
import logging
from core import globals
from . import main as sa
from enum import Enum
import datetime
from core import alerts
from . import control

class LevelStatus(Enum):
    STATUS_SLEEP = "SLEEP"
    STATUS_AWAKE = "AWAKE"
    STATUS_CLOSE = "CLOSE"

class Level(bo.BaseObject):

    scenario = None
    controls = []
    internal_threat_level = 0
    launch_threat_level = 0
    threat_to_scenario = 0
    ttl = 0
    key_attributes = []
    aggregation = []
    next_level = None

    def __init__(self):
        super(Level, self).__init__()
        self.logger = logging.getLogger('Level')
        self.scenario = None
        self.controls = []
        self.internal_threat_level = 0
        self.launch_threat_level = 0
        self.threat_to_scenario = 0
        self.ttl = 0
        self.key_attributes = []
        self.aggregation = None
        self.next_level = None

    @staticmethod
    def create_level(l):
        level = Level()
        level.internal_id = l["_id"]
        level.name = l["name"] if "name" in l.keys() else ""
        level.description = l["description"] if "description" in l.keys() else ""
        level.internal_threat_level = l["internal_threat_level"] if "internal_threat_level" in l.keys() else 0
        level.launch_threat_level = l["launch_threat_level"] if "launch_threat_level" in l.keys() else 0
        level.threat_to_scenario = l["threat_to_scenario"] if "threat_to_scenario" in l.keys() else 0
        level.ttl = l["ttl"] if "ttl" in l.keys() else 0
        level.key_attributes = l["key_attributes"] if "key_attributes" in l.keys() else []
        return level

    def analyze_event(self, data):
        nodes=[]
        for control in self.controls:
            c_nodes = control.analyze_event(data)
            for c_n in c_nodes:
                if c_n["created"]==data.control_time:
                    # New event
                    self.logger.debug("NEW control object")
                    l_nodes = self.search_object()
                    if l_nodes  != None and l_nodes.count() > 0:
                        self.logger.debug("Found levels for this control")
                        for l_n in l_nodes :
                            self.logger.debug("Creating level data for control")
                            levelData = LevelData()
                            levelData.config_object = self
                            levelData.internal_id = self.internal_id
                            levelData.from_json(l_n)
                            levelData.update_object(data)
                            self.relation_nodes(levelData.node, c_n)
                    elif l_nodes != None:
                        self.logger.debug("No level found for this control")
                        levelData = LevelData()
                        levelData.config_object = self
                        levelData.internal_id = self.internal_id
                        self.new_level(levelData, data)
                        self.relation_nodes(levelData.node, c_n)
                else:
                    self.logger.debug("NOT NEW control object")
                    level_nodes = self.search_object_from_control(c_n)
                    for l_n in level_nodes:
                        levelData = LevelData()
                        levelData.from_json(l_n)
                        levelData.update_object(data)
                nodes.append(c_n["_id"])
        return nodes

    def search_object_from_control(self, control):
        aql="FOR v "\
            "IN inbound '"+control["_id"]+"' "\
            "GRAPH '"+sa.ScenarioAnalysis.SCENARIO_GRAPH+"' "\
            "return v"
        nodes = globals.db.ExecuteQuery(aql)
        return nodes

    def search_object(self):
        self.logger.debug("Searching level for the control")
        aql ="FOR l "\
                "IN "+ sa.ScenarioAnalysis.LEVEL_COLLECTION + " "\
                "FILTER l.internal_id=='"+self.internal_id+"' AND "\
                + "l.status == '"+ LevelStatus.STATUS_AWAKE.value +"' "\
                "RETURN l"
        nodes = globals.db.ExecuteQuery(aql)
        return nodes

    def new_level(self, level, data):
        self.logger.debug("New level %s", self.name)
        level.new_object(data)
        if self.scenario != None and self.scenario.first_level == self:
            _scenario = self.scenario.new_scenario(data)
            globals.db.CreateEdge(graph=sa.ScenarioAnalysis.SCENARIO_GRAPH, _from=_scenario.node["_id"], _to=level.node["_id"],
                                  edge=sa.ScenarioAnalysis.SCENARIO_EDGES, data=None,
                                  label=sa.ScenarioAnalysis.RELATION_SCENARIO_LEVEL)
        if self.next_level != None:
            nl = self.next_level.new_empty_level()
            globals.db.CreateEdge(graph=sa.ScenarioAnalysis.SCENARIO_GRAPH, _from=level.node["_id"], _to=nl.node["_id"],
                                  edge=sa.ScenarioAnalysis.SCENARIO_EDGES, data=None,
                                  label=sa.ScenarioAnalysis.RELATION_LEVEL_LEVEL)

    def new_empty_level(self):
        self.logger.debug("Creating level %s", self.name)
        level = LevelData()
        level.config_object = self
        level.internal_id = self.internal_id
        level.create_level()
        if self.next_level != None:
            nl = self.next_level.new_empty_level()
            globals.db.CreateEdge(graph=sa.ScenarioAnalysis.SCENARIO_GRAPH, _from=level.node["_id"], _to=nl.node["_id"],
                                  edge=sa.ScenarioAnalysis.SCENARIO_EDGES, data=None,
                                  label=sa.ScenarioAnalysis.RELATION_LEVEL_LEVEL)
        return level

    def relation_nodes(self, level, control):
        self.logger.debug("Creating the relation netween level and control")
        globals.db.CreateEdge(graph=sa.ScenarioAnalysis.SCENARIO_GRAPH, _from=level["_id"], _to=control["_id"], edge=sa.ScenarioAnalysis.SCENARIO_EDGES, data=None, label=sa.ScenarioAnalysis.RELATION_LEVEL_CONTROL)

class LevelData(bo.BaseObjectData, alerts.AlertableLogicInterface):

    awake_time = None
    close_time = None
    status = None
    attributes_mapping = {}
    threat = 0

    def __init__(self):
        super(LevelData, self).__init__()
        self.logger = logging.getLogger('LevelData')
        self.awake_time = datetime.datetime.now().timestamp()
        self.close_time = None
        self.threat = 0
        self.status = LevelStatus.STATUS_SLEEP
        self.attributes_mapping = {}
        self.zaingo_logic_level = 1
        self.zaingo_logic_type = sa.ScenarioAnalysis.ALERT_TYPE_LEVEL

    def from_json(self, data):
        self.logger.debug("Creating level data from json")
        super(LevelData, self).from_json(data)
        self.awake_time = data["awake_time"] if "awake_time" in data.keys() else datetime.datetime.now().timestamp()
        self.close_time = data["close_time"] if "close_time" in data.keys() else None
        self.threat = data["threat"] if "threat" in data.keys() else 0
        self.status = LevelStatus(data["status"]) if "status" in data.keys() else LevelStatus.STATUS_SLEEP
        self.attributes_mapping = data["attributes_mapping"] if "attributes_mapping" in data.keys() else {}
        self.zaingo_logic_id = data["zaingo_logic_id"] if "zaingo_logic_id" in data.keys() else ""

    def to_json(self):
        self.logger.debug("Creating json from object")
        data = {}

        data = super(LevelData, self).to_json()
        data["awake_time"] = self.awake_time
        data["close_time"] = self.close_time
        data["threat"] = self.threat
        data["status"] = self.status.value
        data["attributes_mapping"] = self.attributes_mapping
        data["zaingo_logic_id"] = self.zaingo_logic_id

        return data

    def create_level(self):
        node = self.to_json()
        self.node = node
        node = globals.db.CreateDocument(sa.ScenarioAnalysis.LEVEL_COLLECTION, self.node)
        self.node["_key"] = node["_key"]
        self.node["_id"] = node["_id"]

    def new_object(self, data):
        self.logger.debug("Creating node")

        self.status = LevelStatus.STATUS_AWAKE
        self.created = data.control_time

        self.compose_object(data)

        self.logger.debug("Calling to data base")
        node=self.to_json()
        self.node=node
        node = globals.db.CreateDocument(sa.ScenarioAnalysis.LEVEL_COLLECTION, self.node)
        self.node["_key"] = node["_key"]
        self.node["_id"] = node["_id"]

    def update_object(self, data):
        if data != None:
            self.compose_object(data)
        globals.db.CreateDocument(sa.ScenarioAnalysis.LEVEL_COLLECTION, self.to_json())

    def compose_object(self, data):
        self.modified = data.control_time
        for key in self.config_object.key_attributes:
            try:
                if key not in self.attributes_mapping.keys():
                    self.attributes_mapping[key] = []
                if data.has_attribute(key):
                    value = data.get_attribute(key)
                    if value not in self.attributes_mapping[key]:
                        self.attributes_mapping[key].append(value)
            except Exception as e:
                self.logger.error("Key %s not found or error << %s >> ocurred", key, e)

    def generate_logic(self):
        self.logic.resume = self.config_object.name

        data = {}

        data["created"] = self.created
        data["awake_time"] = self.awake_time
        data["close_time"] = self.close_time
        data["threat"] = self.threat
        data["status"] = self.status.value
        data["attributes_mapping"] = self.attributes_mapping
        data["internal_threat_level"] = self.config_object.internal_threat_level
        data["launch_threat_level"] = self.config_object.internal_threat_level
        data["threat_to_scenario"] = self.config_object.internal_threat_level

        self.logic.data = data

    def save_data(self):
        self.update_object(None)

    def get_engine(self):
        return "ScenarioAnalysis"

    def get_id(self):
        return self.node["_id"] if self.node != None else ''

    def get_logics(self):
        self.logger.debug("IN")
        logics = []
        if self.node == None:
            self.logger.debug("Node is null, aborting")
            return logics


        try:
            self.logger.debug("Executing traverse query")
            nodes = globals.db.get_graph(sa.ScenarioAnalysis.SCENARIO_GRAPH).traverse(start_vertex = self.node["_id"], strategy = "depthfirst", edge_uniqueness = "path", direction = "outbound", max_depth = 1)
        except Exception as ex:
            self.logger.error("Error executing traverse -> %s", ex)
            return logics

        if nodes != None:
            for node in nodes["paths"]:
                vertices = node["vertices"]
                self.logger.debug("Vertices on node: %s", vertices)
                for v in vertices:
                    _id = v["_id"]
                    if sa.ScenarioAnalysis.LEVEL_COLLECTION in _id and self.get_id() != _id:
                        if not (self.get_id() in _id):
                            levelData = LevelData()
                            if isinstance(levelData, alerts.AlertableLogicInterface):
                                #levelData.internal_id = v["internal_id"]
                                level_config_db = globals.db.GetDocument(v["internal_id"])
                                if level_config_db !=None:
                                    level_config = Level.create_level(level_config_db)
                                    levelData.config_object = level_config
                                    levelData.from_json(v)
                                    logics.append(levelData)
                    elif sa.ScenarioAnalysis.CONTROL_COLLECTION in _id:
                        controlData = control.ControlData()
                        if isinstance(controlData, alerts.AlertableLogicInterface):
                            #controlData.internal_id = v["internal_id"]
                            control_config_db = globals.db.GetDocument(v["internal_id"])
                            if control_config_db !=None:
                                control_config = control.Control.create_control(control_config_db)
                                controlData.config_object = control_config
                                controlData.from_json(v)
                                logics.append(controlData)
        self.logger.debug("OUT")
        return logics