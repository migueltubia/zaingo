from . import aggregation as agg_import
import logging
from core import globals
from enum import Enum
from core import alerts
import datetime
from . import main as sc
from  . import condition as cond
from plugins import edict

class ControlStatus(Enum):
    STATUS_CLOSED = "CLOSED"
    STATUS_OPEN = "OPEN"
    STATUS_SLEEP = "SLEEP"

class ControlLaunchStatus(Enum):
    STATUS_WAIT = "WAIT"
    STATUS_LAUNCH = "LAUNCH"

class Correlation():

    unitary = True
    condition = None
    controls = None

    def __init__(self):
        super(Correlation, self).__init__()
        self.logger = logging.getLogger('Correlation')
        self.unitary = True
        self.controls = []

    @staticmethod
    def create_correlation(o, internal_id, order):
        cc = Correlation()

        cc.unitary = o["unitary"] if "unitary" in o.keys() else True

        if "condition" in o.keys():
            condition = cond.Condition.create_condition(o["condition"])
            cc.condition = condition

        if "controls" in o.keys():
            for control in o["controls"]:
                c = Control.create_control(control, internal_id, order)
                cc.controls.append(c)

        return cc

    def calculate(self, events, ec):
        matches = []
        for e in events:
            event = edict.edict(e)
            if self.condition.calculate(event, ec):
                matches.append(e)
        return matches

class Control():

    internal_id = ""
    name = ""
    description = ""
    condition = None
    correlation = None

    aggregations = {}
    key_field = []
    next_control = None
    prev_control = None
    max_time = 0
    time_to_life = 0

    order = 0
    code = 0

    def __init__(self):
        self.logger = logging.getLogger('Control')
        self.internal_id = ""
        self.name = ""
        self.description = ""
        self.condition = None
        self.correlation_condition = None

        self.aggregations = {}
        self.key_field = []
        self.condition = None
        self.next_control = None
        self.prev_control = None
        self.max_time = 0
        self.time_to_life = 0

        self.order = 0
        self.code = 0

    def evaluate_condition(self, data):
        if self.condition != None:
            return self.condition.calculate(data)

    @staticmethod
    def create_control(o, internal_id, order):
        control = Control()

        control.order = order
        control.name = o["name"] if "name" in o.keys() else ""
        control.description = o["description"]  if "description" in o.keys() else ""
        control.key_field = o["key_field"] if "key_field" in o.keys() else []
        control.max_time = o["max_time"] if "max_time" in o.keys() else 0
        control.time_to_life = o["time_to_life"] if "time_to_life" in o.keys() else 0
        control.code = o["code"] if "code" in o.keys() else 0

        aggs = o["aggregations"] if "aggregations" in o.keys() else []

        if aggs != []:
            for a in aggs:
                new_agg = agg_import.Aggregation.create_aggregation(a)
                control.aggregations[new_agg.code] = new_agg

        condition = o["condition"] if "condition" in o.keys() else None
        if condition != None:
            control.condition = cond.Condition.create_condition(condition)

        correlation = o["correlation"] if "correlation" in o.keys() else None
        if correlation != None:
            control.correlation = Correlation.create_correlation(correlation, internal_id, order + 1)
            for _control in control.correlation.controls:
                _control.prev_control = control

        control.internal_id = internal_id + "/" + str(control.order) + "/" + str(control.code)

        if control.internal_id not in sc.SimpleCorrelation.objects.keys():
            sc.SimpleCorrelation.objects[control.internal_id] = control

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
                        dataC.config_object = self
                        dataC.update_object(data)
                        used_in.append(dataC.node)
        else:
            self.logger.warning("Condition not set")

        nodes = self.get_all_nodes()
        while nodes.has_more():
            node = nodes.next()
            self.logger.debug("Loading node %s", node)
            dataC = ControlData()
            dataC.from_json(node)
            dataC.config_object = self
            matches = dataC.correlate(data)
            if matches != None and len(matches) > 0:
                for control in self.correlation.controls:
                    new_nodes = control.analyze_event(data)
                    for _new in new_nodes:
                        globals.db.CreateEdge(graph = sc.SimpleCorrelation.GRAPH, _from = dataC.node["_id"],
                                          _to = _new.node["_id"],
                                          edge = sc.SimpleCorrelation.EDGES, data = None,
                                          label = sc.SimpleCorrelation.RELATION_CONTROL_CONTROL)
                    used_in += new_nodes
        return used_in

    def get_all_nodes(self):
        self.logger.debug("IN: get all nodes")
        aql = "FOR c " + \
              "IN " + sc.SimpleCorrelation.CONTROL_COLLECTION + " " + \
              "FILTER c.internal_id=='" + self.internal_id + "'AND c.status == '" + ControlStatus.STATUS_OPEN.value + "' " + \
              "RETURN c"
        nodes = globals.db.ExecuteQuery(aql)
        self.logger.debug("OUT: get all nodes")
        return nodes

    def search_object(self, data):
        self.logger.debug("IN: searching existing node")
        values = self.get_fields(data)
        aql ="FOR c " + \
                "IN "+ sc.SimpleCorrelation.CONTROL_COLLECTION + " " + \
                "FILTER c.internal_id=='" + self.internal_id + "' AND " + \
                " " + str(values) + " IN c.key_value AND c.status == '" + ControlStatus.STATUS_OPEN.value + "' " + \
                "RETURN c"
        nodes = globals.db.ExecuteQuery(aql)
        self.logger.debug("OUT: searching existing node")
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

class ControlData(alerts.AlertableLogicInterface):

    node = None
    config_object = None
    created = None
    modified = None
    status = None
    launch_status = None
    internal_id = ""

    aggregations = {}
    key_value = []

    events = []

    def __init__(self):
        super(ControlData, self).__init__()
        self.logger = logging.getLogger('ControlData')

        self.node = None
        self.config_object = None
        self.created = datetime.datetime.now().timestamp()
        self.modified = datetime.datetime.now().timestamp()
        self.status = ControlStatus.STATUS_SLEEP
        self.launch_status = ControlLaunchStatus.STATUS_WAIT
        self.internal_id = ""

        self.aggregations = {}
        self.key_value = []

        self.events = []

        self.zaingo_logic_level = 2
        self.zaingo_logic_type = sc.SimpleCorrelation.ALERT_TYPE_CONTROL

    def correlate(self, data):
        correlation = self.config_object.correlation
        if correlation == None:
            return None
        matches = correlation.calculate(self.events, data)
        return matches

    def from_json(self, data):
        self.logger.debug("IN: %s", data)

        self.internal_id = data["internal_id"] if "internal_id" in data.keys() else ""
        self.created = data["created"] if "created" in data.keys() else datetime.datetime.now().timestamp()
        self.modified = data["modified"] if "modified" in data.keys() else datetime.datetime.now().timestamp()
        self.config_object = sc.SimpleCorrelation.objects[self.internal_id] if self.internal_id != "" else None
        self.node = data

        self.key_value = data["key_value"] if "key_value" in data.keys() else []
        self.status = ControlStatus(data["status"]) if "status" in data.keys() else ControlStatus.STATUS_OPEN
        self.launch_status = ControlLaunchStatus(data["launch_status"]) if "launch_status" in data.keys() else ControlLaunchStatus.STATUS_WAIT

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

        self.events = data["events"] if "events" in data.keys() else []

        self.zaingo_logic_id = data["zaingo_logic_id"] if "zaingo_logic_id" in data.keys() else ""

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

        data["key_value"] = self.key_value
        data["aggregations"] = []

        for key, aggregation in self.aggregations.items():
            agg = aggregation.to_json()
            data["aggregations"].append(agg)

        data["events"] = self.events

        data["order"] = self.config_object.order

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
        node = globals.db.CreateDocument(sc.SimpleCorrelation.CONTROL_COLLECTION, self.node)
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
            agg.do(data)


    def update_object(self, data):
        if data != None:
            self.compose_object(data)
        globals.db.CreateDocument(sc.SimpleCorrelation.CONTROL_COLLECTION, self.to_json())

    def compose_object (self, data):
        self.modified = data.control_time
        self.events.append(data)
        self.logger.debug("Getting aggregations")
        self.aggregate(data)

    def generate_logic(self):
        self.logic.resume = self.config_object.name

        data = {}

        data["created"] = self.created

        data["key_value"] = self.key_value
        data["aggregations"] = []
        data["status"] = self.status.value

        for key, aggregation in self.aggregations.items():
            agg = aggregation.to_json()
            data["aggregations"].append(agg)

        self.logic.data = data

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
                        controlData = ControlData()
                        if isinstance(controlData, alerts.AlertableLogicInterface):
                            control_config_db = globals.db.GetDocument(v["internal_id"])
                            if control_config_db !=None:
                                control_config = Control.create_control(control_config_db)
                                controlData.config_object = control_config
                                controlData.from_json(v)
                                logics.append(controlData)
        return logics