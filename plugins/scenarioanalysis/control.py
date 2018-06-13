from . import base_object as bo
from . import aggregation as agg_import
from . import main as sa
import logging
from core import globals
from enum import Enum
from core import alerts

class ControlStatus(Enum):
    STATUS_CLOSED = "CLOSED"
    STATUS_OPEN = "OPEN"

class Control(bo.BaseObject):

    level = None
    aggregations = {}
    key_field = []
    threat = 0
    condition = None
    next_control = None
    preload = False
    max_time = 0
    time_to_life = 0

    def __init__(self):
        super(Control, self).__init__()
        self.logger = logging.getLogger('Control')
        self.level = None
        self.aggregations = {}
        self.key_field = []
        self.threat = 0
        self.condition = None
        self.next_control = None
        self.preload = False
        self.max_time = 0
        self.time_to_life = 0

    @staticmethod
    def create_control(o):
        control = Control()
        control.internal_id = o["_id"]
        control.name = o["name"] if "name" in o.keys() else ""
        control.description = o["description"]  if "description" in o.keys() else ""
        control.key_field = o["key_field"] if "key_field" in o.keys() else []
        control.threat = o["threat"] if "threat" in o.keys() else 0
        control.preload = o["preload"] if "preload" in o.keys() else False
        control.max_time = o["max_time"] if "max_time" in o.keys() else 0
        control.time_to_life = o["time_to_life"] if "time_to_life" in o.keys() else 0

        aggs = o["aggregations"] if "aggregations" in o.keys() else []

        if aggs != []:
            for a in aggs:
                new_agg = agg_import.Aggregation.create_aggregation(a)
                control.aggregations[new_agg.code] = new_agg

        return control

    def analyze_event(self, data):
        self.logger.debug("Analyzing event in control %s", self.name)
        used_in = []
        if self.condition != None:
           if self.condition.calculate(data):
                self.logger.debug("Condition passed")
                nodes = self.search_object(data)
                if nodes.count() == 0:
                    self.logger.debug("No object found")
                    dataC = ControlData()
                    dataC.internal_id = self.internal_id
                    dataC.config_object = self
                    dataC.new_object(data)
                    used_in.append(dataC.node)
                else:
                    computed = 0
                    while computed < nodes.count():
                        computed += 1
                        node = nodes.next()
                        dataC = ControlData()
                        dataC.from_json(node)
                        dataC.update_object(data)
                        used_in.append(dataC.node)
        return used_in

    def search_object(self, data):
        self.logger.debug("Searching existing node")
        values = self.get_fields(data)
        aql ="FOR c " + \
                "IN "+ sa.ScenarioAnalysis.CONTROL_COLLECTION + " " + \
                "FILTER c.internal_id=='" + self.internal_id + "' AND " + \
                " " + str(values) + " IN c.key_value AND c.status == '" + ControlStatus.STATUS_OPEN.value + "' " + \
                "RETURN c"
        nodes = globals.db.ExecuteQuery(aql)
        return nodes

    def get_fields(self, data):
        self.logger.debug("Setting key fields")
        values = {}
        if self.key_field != []:
            values = {}
            for key in self.key_field:
                self.logger.debug("Setting field %s", key)
                if data.has_attribute(key):
                    values[key] = data.get_attribute(key)
                    self.logger.debug("Key found")
                else:
                    self.logger.debug("Data has not the attribute")
        return values

class ControlData(bo.BaseObjectData, alerts.AlertableLogicInterface):

    aggregations = {}
    key_value = []
    threat = 0

    def __init__(self):
        super(ControlData, self).__init__()
        self.logger = logging.getLogger('ControlData')
        self.aggregations = {}
        self.key_value = []
        self.threat = 0
        self.zaingo_logic_level = 2
        self.zaingo_logic_type = sa.ScenarioAnalysis.ALERT_TYPE_CONTROL

    def from_json(self, data):
        self.logger.debug("Creating control data from json")
        super(ControlData, self).from_json(data)
        self.key_value = data["key_value"] if "key_value" in data.keys() else []
        self.threat = data["threat"] if "threat" in data.keys() else 0
        self.status = ControlStatus(data["status"]) if "status" in data.keys() else ControlStatus.STATUS_OPEN
        aggs = data["aggregations"] if "aggregations" in data.keys() else {}
        if aggs != []:
            for a in aggs:
                self.logger.debug("Creating agg %s for control", a)
                agg_data = agg_import.AggregationData()
                agg_data.from_json(a)
                if agg_data.status == agg_import.AggregationStatus.STATUS_OPEN:
                    self.logger.debug("Importing agg with status Open")
                    agg_config = self.config_object.aggregations[agg_data.code]
                    agg_data.aggregation = agg_config
                    self.aggregations[str(agg_data.field_values)] = agg_data

        self.zaingo_logic_id = data["zaingo_logic_id"] if "zaingo_logic_id" in data.keys() else ""

    def to_json(self):
        self.logger.debug("Creating json from object")
        data={}

        data=super(ControlData, self).to_json()
        data["key_value"] = self.key_value
        data["threat"] = self.threat
        data["aggregations"] = []
        data["status"] = self.status.value

        for key, aggregation in self.aggregations.items():
            agg = aggregation.to_json()
            data["aggregations"].append(agg)

        data["zaingo_logic_id"] = self.zaingo_logic_id

        return data

    def new_object(self, data):
        self.logger.debug("Creating node")

        self.status = ControlStatus.STATUS_OPEN
        self.created = data.control_time

        values = self.config_object.get_fields(data)
        if values != {}:
            if values not in self.key_value:
                self.key_value.append(values)

        self.compose_object(data)

        self.logger.debug("Calling to data base")
        node = self.to_json()
        self.node = node
        node = globals.db.CreateDocument(sa.ScenarioAnalysis.CONTROL_COLLECTION, self.node)
        self.node["_key"] = node["_key"]
        self.node["_id"] = node["_id"]

    def aggregate(self, data):
        self.logger.debug("Aggregating data")
        if self.config_object == None or self.config_object.aggregations == {}:
            self.logger.debug("No aggregations found")
            return

        for code, aggregation in self.config_object.aggregations.items():
            values = aggregation.get_fields(data)
            agg = None
            if str(values) in self.aggregations.keys():
                self.logger.debug("Previous agg found")
                agg = self.aggregations[str(values)]
            else:
                self.logger.debug("Creating new aggregation")
                agg = agg_import.AggregationData()
                agg.create(aggregation, data)
                self.aggregations[str(agg.field_values)] = agg
            self.logger.debug("Calling to execute the aggregation data")
            self.threat += agg.do(data)


    def update_object(self, data):
        if data != None:
            self.compose_object(data)
        globals.db.CreateDocument(sa.ScenarioAnalysis.CONTROL_COLLECTION, self.to_json())

    def compose_object (self, data):
        self.modified = data.control_time
        self.logger.debug("Getting aggregations")
        self.aggregate(data)

    def generate_logic(self):
        self.logic.resume = self.config_object.name

        data = {}

        data["created"] = self.created

        data["key_value"] = self.key_value
        data["threat"] = self.threat
        data["aggregations"] = []
        data["status"] = self.status.value

        for key, aggregation in self.aggregations.items():
            agg = aggregation.to_json()
            data["aggregations"].append(agg)

        self.logic.data = data

    def save_data(self):
        self.update_object(None)

    def get_engine(self):
        return "ScenarioAnalysis"

    def get_id(self):
        return self.node["_id"] if self.node != None else ''

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
                    if sa.ScenarioAnalysis.CONTROL_COLLECTION in _id and self.get_id() != _id:
                        controlData = ControlData()
                        if isinstance(controlData, alerts.AlertableLogicInterface):
                            control_config_db = globals.db.GetDocument(v["internal_id"])
                            if control_config_db !=None:
                                control_config = Control.create_control(control_config_db)
                                controlData.config_object = control_config
                                controlData.from_json(v)
                                logics.append(controlData)
        return logics