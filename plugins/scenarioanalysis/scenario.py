from . import base_object as bo
import logging
from core import globals
from . import main as sa
from enum import Enum
from core import alerts
from . import level

class ScenarioStatus(Enum):
    STATUS_OPEN = "OPEN"
    STATUS_CLOSE = "CLOSE"

class Scenario(bo.BaseObject):

    purpose = ""
    threat_level = 0
    priority = None
    first_level = None
    category = ""

    def __init__(self):
        super(Scenario, self).__init__()
        self.logger = logging.getLogger('Scenario')
        self.purpose = ""
        self.threat_level = 0
        self.priority = Priority()
        self.first_level  = None
        self.category = ""


    @staticmethod
    def create_scenario(s):
        scenario = Scenario()
        scenario.internal_id = s["_id"]
        scenario.name = s["name"] if "name" in s.keys() else ""
        scenario.description = s["description"] if "description" in s.keys() else ""
        scenario.purpose = s["purpose"] if "purpose" in s.keys() else ""
        scenario.category = s["category"] if "category" in s.keys() else ""
        scenario.threat_level = s["threat_level"] if "threat_level" in s.keys() else 0
        if "priority" in s.keys():
            p = Priority()
            pv = s["priority"]
            for k in pv:
                p.modify_value(k, min(pv[k]), max(pv[k]))
            scenario.priority = p
        return scenario

    def get_priority(self, threat):
        return self.priority.get_priority(threat)

    def analyze_event(self, data):
        nodes = []
        if self.first_level != None:
            node = self.first_level.analyze_event(data)
            if node != []:
                nodes += node
        return nodes

    def new_scenario(self, data):
        _data = ScenarioData ()
        _data.config_object = self
        _data.internal_id = self.internal_id
        _data.new_object(data)
        return _data


class Priority():
    low = (1,10)
    medium = (11,25)
    high = (26,60)
    critical = (61,90)
    paranoia = (91,100)

    level = {}

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
    PARANOIA = "PARANOIA"

    def __init__(self):
        self.level[Priority.LOW] = self.low
        self.level[Priority.MEDIUM] = self.medium
        self.level[Priority.HIGH] = self.high
        self.level[Priority.CRITICAL] = self.critical
        self.level[Priority.PARANOIA] = self.paranoia

    def get_priority(self, threat):
        p=Priority.LOW
        for k, v in self.level.items():
            if min(v) <= threat <= max(v):
                p=k
        return p

    def modify_value(self, type, _from, _to):
        if type in self.level:
            self.level[type.upper()] = (_from, _to)

class ScenarioData(bo.BaseObjectData, alerts.AlertableInterface):
    threat = 0

    def __init__(self):
        super(ScenarioData, self).__init__()
        self.logger = logging.getLogger('ScenarioData')
        self.threat = 0
        self.status = ScenarioStatus.STATUS_OPEN
        self.alert = None

    def from_json(self, data):
        self.logger.debug("Creating scenario data from json")
        super(ScenarioData, self).from_json(data)
        self.threat = data["threat"] if "threat" in data.keys() else 0
        self.status = ScenarioStatus(data["status"]) if "status" in data.keys() else ScenarioStatus.STATUS_OPEN
        self.zaingo_alert_id = data["zaingo_alert_id"] if "zaingo_alert_id" in data.keys() else ""

    def to_json(self):
        self.logger.debug("Creating json from object")
        data={}

        data = super(ScenarioData, self).to_json()
        data["threat"] = self.threat
        data["status"] = self.status.value
        data["zaingo_alert_id"] = self.zaingo_alert_id

        return data

    def new_object(self, data):
        self.logger.debug("Creating node")

        self.created = data.control_time

        self.logger.debug("Calling to data base")
        node=self.to_json()
        self.node=node
        node = globals.db.CreateDocument(sa.ScenarioAnalysis.SCENARIO_COLLECTION, self.node)
        self.node["_key"] = node["_key"]
        self.node["_id"] = node["_id"]

    def update_object(self, data):
        if data != None:
            self.modified = data.control_time
        globals.db.CreateDocument(sa.ScenarioAnalysis.SCENARIO_COLLECTION, self.to_json())

    def generate_alert(self):
        self.logger.debug("IN: Generating alert")
        self.alert.name = self.config_object.name
        self.alert.description = self.config_object.description
        self.alert.category = self.config_object.category
        self.alert.severity = self.config_object.get_priority(self.threat)
        self.alert.data["threat"] = self.threat
        self.alert.data["threat_level"] = self.config_object.threat_level
        self.logger.debug("OUT: Internal generating alert")

    def get_logics(self):
        if self.node == None:
            return

        logics = []

        nodes = globals.db.get_graph(sa.ScenarioAnalysis.SCENARIO_GRAPH).traverse(start_vertex = self.node["_id"], strategy = "depthfirst", edge_uniqueness = "path", direction = "outbound", max_depth = 1)

        if nodes != None:
            for node in nodes["paths"]:
                vertices = node["vertices"]
                self.logger.debug("Vertices on node: %s", vertices)
                for v in vertices:
                    _id = v["_id"]
                    if sa.ScenarioAnalysis.LEVEL_COLLECTION in _id:
                        levelData = level.LevelData()
                        if isinstance(levelData, alerts.AlertableLogicInterface):
                            levelData.internal_id = v["internal_id"]
                            level_config_db = globals.db.GetDocument(v["internal_id"])
                            if level_config_db !=None:
                                level_config = level.Level.create_level(level_config_db)
                                levelData.config_object = level_config
                                levelData.from_json(v)
                                logics.append(levelData)
        return logics

    def save_data(self):
        self.update_object(None)