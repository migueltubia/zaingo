import logging
from core import globals
from enum import Enum
from core import alerts
import datetime
from . import main as sc
from . import control

class RuleStatus(Enum):
    STATUS_CLOSED = "CLOSED"
    STATUS_OPEN = "OPEN"

class RuleLaunchStatus(Enum):
    STATUS_WAIT = "WAIT"
    STATUS_LAUNCH = "LAUNCH"

class Rule():

    internal_id = ""
    name = ""
    description = ""

    condition = None

    purpose = ""
    priority = 0
    first_control = None
    category = ""

    max_time = 0
    time_to_life = 0

    def __init__(self):
        self.logger = logging.getLogger('Rule')

        self.internal_id = ""
        self.name = ""
        self.description = ""

        self.condition = None

        self.purpose = ""
        self.priority = 0
        self.first_control = None
        self.category = ""

        self.max_time = 0
        self.time_to_life = 0

    def evaluate_condition(self, data):
        if self.condition == None:
            return self.internal_condition.calculate(data)
        else:
            return self.condition.calculate(data)

    @staticmethod
    def create_rule(o):
        rule = Rule()

        rule.internal_id = o["_id"]
        rule.name = o["name"] if "name" in o.keys() else ""
        rule.description = o["description"]  if "description" in o.keys() else ""

        rule.purpose = o["purpose"] if "purpose" in o.keys() else ""
        rule.priority = o["priority"] if "priority" in o.keys() else 0
        rule.category = o["category"] if "category" in o.keys() else ""

        rule.max_time = o["max_time"] if "max_time" in o.keys() else 0
        rule.time_to_life = o["time_to_life"] if "time_to_life" in o.keys() else 0

        controls = o["controls"] if "controls" in o.keys() else None
        if controls != None:
            _control = control.Control.create_control(controls, rule.internal_id, 1)
            rule.first_control = _control

        return rule

    def analyze_event(self, data):
        self.logger.debug("IN")
        nodes = []
        if self.first_control != None:
            node = self.first_control.analyze_event(data)
            if len(node) > 0:
                nodes += node
        if len(nodes) > 0:
            self.logger.debug("Nodes effected, searching rule")
            _rules = self.search_object()
            _rule = None
            if _rules.count() > 0:
                _rule_j = _rules.next()
                _rule = RuleData()
                _rule.from_json(_rule_j)
                _rule.config_object = self
            if _rule == None:
                self.logger.debug("Rule not found, creating")
                _rule = self.new_rule(data)
            else:
                _rule.update_object(data)
            for _node in nodes:
                self.logger.debug("Processing node control %s", _node)
                if _node["order"] == 1:
                    globals.db.CreateEdge(graph=sc.SimpleCorrelation.GRAPH, _from=_rule.node["_id"],
                                          _to=_node["_id"],
                                          edge=sc.SimpleCorrelation.EDGES, data = None,
                                          label=sc.SimpleCorrelation.RELATION_RULE_CONTROL)
        self.logger.debug("OUT: %s", nodes)
        return nodes

    def new_rule(self, data):
        self.logger.debug("IN")
        _data = RuleData ()
        _data.config_object = self
        _data.internal_id = self.internal_id
        _data.new_object(data)
        self.logger.debug("OUT: %s", _data)
        return _data

    def search_object(self):
        self.logger.debug("Searching existing rule")
        aql ="FOR c " + \
                "IN "+ sc.SimpleCorrelation.RULE_COLLECTION + " " + \
                "FILTER c.internal_id=='" + self.internal_id + "' AND c.status == '" + RuleStatus.STATUS_OPEN.value + "' " + \
                "RETURN c"
        nodes = globals.db.ExecuteQuery(aql)
        return nodes

class RuleData(alerts.AlertableInterface):

    node = None
    config_object = None
    created = None
    modified = None
    status = None
    launch_status = None
    internal_id = ""


    def __init__(self):
        super(RuleData, self).__init__()
        self.logger = logging.getLogger('RuleData')

        self.node = None
        self.config_object = None
        self.created = datetime.datetime.now().timestamp()
        self.modified = datetime.datetime.now().timestamp()
        self.status = RuleStatus.STATUS_OPEN
        self.launch_status = RuleLaunchStatus.STATUS_WAIT
        self.internal_id = ""

    def from_json(self, data):
        self.logger.debug("IN %s", data)

        self.internal_id = data["internal_id"] if "internal_id" in data.keys() else self.internal_id
        self.created = data["created"] if "created" in data.keys() else datetime.datetime.now().timestamp()
        self.modified = data["modified"] if "modified" in data.keys() else datetime.datetime.now().timestamp()
        self.config_object = sc.SimpleCorrelation.objects[self.internal_id] if self.internal_id != "" else None
        self.node = data

        self.status = RuleStatus(data["status"]) if "status" in data.keys() else RuleStatus.STATUS_OPEN
        self.launch_status = RuleLaunchStatus(data["launch_status"]) if "launch_status" in data.keys() else RuleLaunchStatus.STATUS_WAIT

        self.zaingo_alert_id = data["zaingo_alert_id"] if "zaingo_alert_id" in data.keys() else ""
        self.logger.debug("OUT")

    def to_json(self):
        self.logger.debug("Creating json from object")
        data={}

        data["internal_id"] = self.internal_id
        data["created"] = self.created
        data["modified"] = self.modified
        data["status"] = self.status.value
        data["launch_status"] = self.launch_status.value
        if self.node != None:
            data["_id"] = self.node["_id"]
            data["_key"] = self.node["_key"]

        data["zaingo_alert_id"] = self.zaingo_alert_id

        return data

    def new_object(self, data):
        self.logger.debug("Creating node")

        self.status = RuleStatus.STATUS_OPEN
        self.created = data.control_time

        self.compose_object(data)

        self.logger.debug("Calling to data base")
        node = self.to_json()
        self.node = node
        node = globals.db.CreateDocument(sc.SimpleCorrelation.RULE_COLLECTION, self.node)
        self.node["_key"] = node["_key"]
        self.node["_id"] = node["_id"]

    def update_object(self, data):
        if data != None:
            self.compose_object(data)
        globals.db.CreateDocument(sc.SimpleCorrelation.RULE_COLLECTION, self.to_json())

    def compose_object (self, data):
        self.modified = data.control_time

    def generate_alert(self):
        self.logger.debug("IN: Generating alert")
        self.alert.name = self.config_object.name
        self.alert.description = self.config_object.description
        self.alert.category = self.config_object.category
        self.alert.severity = self.config_object.priority
        self.logger.debug("OUT: Generating alert")

    def save_data(self):
        self.update_object(None)

    def get_engine(self):
        return "SimpleCorrelation"

    def get_id(self):
        return self.node["_id"] if self.node != None else ''

    def get_logics(self):
        if self.node == None:
            return

        logics = []

        nodes = globals.db.get_graph(sc.SimpleCorrelation.GRAPH).traverse(start_vertex = self.node["_id"], strategy = "depthfirst", edge_uniqueness = "path", direction = "outbound", max_depth = 1)

        if nodes != None:
            for node in nodes["paths"]:
                vertices = node["vertices"]
                self.logger.debug("Vertices on node: %s", vertices)
                for v in vertices:
                    _id = v["_id"]
                    if sc.SimpleCorrelation.CONTROL_COLLECTION in _id and self.get_id() != _id:
                        controlData = control.ControlData()
                        if isinstance(controlData, alerts.AlertableLogicInterface):
                            control_config_db = globals.db.GetDocument(v["internal_id"])
                            if control_config_db !=None:
                                control_config = control.Control.create_control(control_config_db)
                                controlData.config_object = control_config
                                controlData.from_json(v)
                                logics.append(controlData)
        return logics