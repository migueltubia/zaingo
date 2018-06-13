import datetime
import logging
from core import globals
from plugins import out_filter as ou
from plugins import OutputPlugin as op
from enum import Enum

ALERTS_COLLECTION_NAME = "alerts_collection"
EVENTS_COLLECTION_NAME = "alerts_events"
LOGIC_COLLECTION_NAME = "alerts_logic"
GRAPH_NAME = "alerts_graph"
EDGES_COLLECTION_NAME = "alerts_edges"

EDGE_LOGIC = "follow_by"
EDGE_EVENT = "affect_to"
EDGE_ALERT = "composed_by"

class AlertType(Enum):
    TYPE_ALERT = "Alert"
    TYPE_WARNING = "Warning"
    TYPE_INFO = "Info"

class AlertableInterface(object):

    zaingo_alert_id = ""

    alert = None

    def __init__(self):
        self.logger = logging.getLogger('AlertableInterface')
        super().__init__()
        self.zaingo_alert_id = ""

        self.alert = Alert()

    def execute(self):
        self.__generate_alert()

    def generate_alert(self):
        pass

    def __generate_alert(self):
        self.logger.debug("Internal generating alert")
        if self.zaingo_alert_id != "":
            self.logger.debug("alert_id is not null: %s", self.zaingo_alert_id)
            self.alert = Alert.load_alert(self.zaingo_alert_id)
        else:
            self.alert = Alert()
        self.logger.debug("Calling generating alert")
        self.generate_alert()
        self.logger.debug("Calling save")
        self.alert.save()
        self.zaingo_alert_id = self.alert.id
        self.save_data()

        self.__get_logics()

        self.manage_outs()

    def __get_logics(self):
        self.logger.debug("Internal getting logics")
        logics = self.get_logics()
        for logic in logics:
            if isinstance(logic, AlertableLogicInterface):
                logic.parent = self.alert
                logic.execute()

    def save_data(self):
        pass

    def get_logics(self):
        logics = []
        return logics

    def manage_outs(self):
        filters = ou.OutFilter.search_filter_by_alert(self.alert)
        for f in filters:
            for k, v in f.outs.items():
                for c in v:
                    self.logger.debug("Creating out plugin %s with config %s", k, c)
                    p = op.OutputPlugin.load_plugin(k, c)
                    try:
                        p.execute(self.alert)
                    except Exception as error:
                        self.logger.error("Error executing output: %s", error)

class AlertableLogicInterface(object):

    zaingo_logic_id = ""
    zaingo_logic_level = 1
    zaingo_logic_type = "logic"

    logic = None
    parent = None

    def __init__(self):
        super().__init__()
        self.zaingo_logic_id = ""
        self.zaingo_logic_level = 1
        self.zaingo_logic_type = "logic"
        self.logger = logging.getLogger('AlertableLogicInterface')

        self.logic = Logic()

    def execute(self):
        self.__generate_logic()

    def __generate_logic(self):
        self.logger.debug("Internal getting logics")
        if self.zaingo_logic_id != "":
            self.logger.debug("Found previous logic")
            self.logic = Logic.load_logic(self.zaingo_logic_id)
        else:
            self.logic = Logic()

        self.logic.parent = self.parent

        self.logic.level = self.zaingo_logic_level
        self.logic.type = self.zaingo_logic_type

        self.logger.debug("Calling generating logic")
        self.generate_logic()

        self.logger.debug("Calling saving")
        self.logic.save()
        self.zaingo_logic_id = self.logic.id
        self.save_data()

        self.get_events()

        self.__get_logics()

    def generate_logic(self):
        pass

    def __get_logics(self):
        logics = self.get_logics()
        for logic in logics:
            if isinstance(logic, AlertableLogicInterface):
                logic.parent = self.logic
                logic.execute()

    def save_data(self):
        pass

    def get_logics(self):
        logics = []
        return logics

    def get_events(self):
        self.logger.debug("IN")
        qs ="FOR e IN events_edges FILTER '" + self.get_engine() + "' IN e.node_engine[*].engine AND '" + self.get_id() + "' IN e.node_engine[*].id RETURN e._from"
        ds = None
        try:
            ds = globals.db.ExecuteQuery(qs)
        except Exception as error:
            self.logger.error("Error finding events %s - %s", error, qs)

        if ds != None:
            for e in ds:
                self.logger.debug("Working on event %s", e)
                event_node = globals.db.GetDocument(e)
                if event_node != None:
                    self.logger.debug("Searching previous event %s", event_node["_key"])
                    event = Event.load_event(event_node["_key"])
                    if event == None:
                        self.logger.debug("Event not found, creating event in alert database")
                        event = Event()
                        event.event_id = event_node["_key"]
                        event.data = event_node["source"]
                        event.save()
                    else:
                        self.logger.debug("Found previous event")
                    qs2 = "FOR e in " + EDGES_COLLECTION_NAME + " FILTER _from == '" + self.logic.node["_id"] + "' AND _to == '" + event.node["_id"] + "' RETURN e"
                    ds2 = None
                    try:
                        ds2 = globals.db.ExecuteQuery(qs2)
                    except Exception as error:
                        self.logger.error("Error finding events %s - %s", error, qs2)

                    if ds2 == None or ds2.count() == 0:
                        globals.db.CreateEdge(GRAPH_NAME, self.logic.node["_id"], event.node["_id"],
                                              EDGES_COLLECTION_NAME, None, EDGE_EVENT)
                else:
                    self.logger.warning("Event nor found!! Matbe is has been removes?. Eventid: %s", e)
        self.logger.debug("OUT")



    def get_engine(self):
        return ""

    def get_id(self):
        return ""

class Alert(object):
    id = ""
    name = ""
    description = ""
    category = ""
    severity = 0
    time = datetime.datetime.now().timestamp()
    engine = ""
    alert_type = None
    node = None
    data = {}
    node_id = ""

    logics = {}

    def __init__(self):
        self.logger = logging.getLogger('Alert')
        self.node = {}
        self.logics = {}
        self.alert_type = AlertType.TYPE_ALERT
        self.data = {}
        self.node_id = ""
        self.id = ""

    @staticmethod
    def load_alert(alert_id):
        alert = Alert()
        ds_temp = globals.db.get_collection(ALERTS_COLLECTION_NAME).get(alert_id)
        if ds_temp != None:
            alert_json = ds_temp
            alert.from_doc(alert_json)
        return alert

    def to_doc(self):
        data_json={}

        if self.node != None:
            data_json = self.node

        data_json['name'] = self.name
        data_json['description'] = self.description
        data_json['category'] = self.category
        data_json['severity'] = self.severity
        data_json['time'] = str(self.time)
        data_json['engine'] = self.engine
        data_json['node_id'] = self.node_id
        data_json['alert_type'] = self.alert_type.value
        data_json['data'] = self.data

        if self.id != "":
            data_json['_key'] = self.id

        return data_json

    def from_doc(self, node):
        self.node = node
        self.name = node['name'] if "name" in node.keys() else ""
        self.description = node['description'] if "description" in node.keys() else ""
        self.category = node['category'] if "category" in node.keys() else ""
        self.severity = node['severity'] if "severity" in node.keys() else ""
        self.time = node['time'] if "time" in node.keys() else datetime.datetime.now().timestamp()
        self.engine = node['engine'] if "engine" in node.keys() else ""
        self.engine = node['node_id'] if "node_id" in node.keys() else ""
        self.alert_type = AlertType(node['alert_type']) if "alert_type" in node.keys() else AlertType.TYPE_ALERT
        self.data = node['data'] if "data" in node.keys() else {}
        self.id = node['_key'] if "_key" in node.keys() else ""

    def get_root_logic(self):
        root_logic = []
        for key, logic in self.logics.items():
            if len(logic.prev_logic) == 0:
                root_logic.append(logic)
        return root_logic

    def save(self):
        if self.id != "":
            ds_temp = globals.db.get_collection(ALERTS_COLLECTION_NAME).get(self.id)
            if ds_temp != None:
                self.logger.debug("Found previous alert")
                self.node = ds_temp

        self.logger.debug("Creating/Updating alert")
        try:
            self.node = globals.db.CreateDocument(ALERTS_COLLECTION_NAME, self.to_doc())
        except Exception as ex:
            self.logger.error("Error creating alert %s", ex)
        self.id = self.node["_key"]

class Logic(object):
    id = ""
    resume = ""
    data = {}
    type = ""
    node = None
    node_id = ""
    level = 1

    prev_logic = []
    next_logic = []

    parent = None
    events = {}

    def __init__(self):
        self.logger = logging.getLogger('Logic')
        self.node = None
        self.type = ""
        self.data = {}
        self.prev_logic = []
        self.next_logic = []
        self.parent = None
        self.events = {}
        self.node_id = ""
        self.level = 1

    @staticmethod
    def load_logic(logic_id):
        logic = Logic()
        ds_temp = globals.db.get_collection(LOGIC_COLLECTION_NAME).get(logic_id)
        if ds_temp != None:
            logic_json = ds_temp
            logic.from_doc(logic_json)
        return logic

    def to_doc(self):
        data_json = {}
        if self.node != None:
            data_json = self.node

        data_json['resume'] = self.resume
        data_json['type'] = self.type
        data_json['node_id'] = self.node_id
        data_json['data'] = self.data
        data_json['level'] = self.level

        if self.id != "":
            data_json['_key'] = self.id

        return data_json

    def from_doc(self, node):
        self.node = node
        self.id = node['_key'] if "_key" in node.keys() else ""
        self.resume = node['resume'] if "resume" in node.keys() else ""
        self.type = node['type'] if "type" in node.keys() else ""
        self.resume = node['node_id'] if "node_id" in node.keys() else ""
        self.data = node['data'] if "data" in node.keys() else {}
        self.level = node['level'] if "level" in node.keys() else 1

    def add_event(self, event):
        if event.id not in self.events.keys():
            event.add_logic(self)
            self.events[event.id] = event

    def save(self):
        if self.id != "":
            ds_temp = globals.db.get_collection(LOGIC_COLLECTION_NAME).get(self.id)
            if ds_temp != None:
                self.node = ds_temp

        self.node = globals.db.CreateDocument(LOGIC_COLLECTION_NAME, self.to_doc())
        if self.id == "":
            self.id = self.node["_key"]
            label = EDGE_ALERT
            if isinstance(self.parent, Logic):
                label = EDGE_LOGIC
            globals.db.CreateEdge(GRAPH_NAME, self.parent.node["_id"], self.node["_id"], EDGES_COLLECTION_NAME, None, label)

class Event:
    id = ""
    event_id = ""
    node = None
    data = {}


    def __init__(self):
        self.logger = logging.getLogger('Event')
        self.id = ""
        self.event_id = ""
        self.node = None
        self.data = {}

    @staticmethod
    def load_event(event_id):
        event = None
        ds_temp = None
        try:
            ds_temp = globals.db.get_collection(EVENTS_COLLECTION_NAME).find({"event_id": event_id})
        except Exception as ex:
            ds_temp =  None

        if ds_temp != None and ds_temp.count() > 0:
            event_json = ds_temp.next()
            event = Event()
            event.from_doc(event_json)
        return event

    def to_doc(self):
        data_json = self.data
        if self.id != "":
            data_json["_key"] = self.id
        
        data_json["event_id"] = self.event_id

        return data_json
    
    def from_doc(self, node):
        self.node = node
        self.id = node['_key'] if "_key" in node.keys() else ""
        self.data = node['data'] if "data" in node.keys() else {}
        self.event_id = node['event_id'] if "event_id" in node.keys() else ""

    def save(self):
        if self.id != "":
            ds_temp = globals.db.get_collection(EVENTS_COLLECTION_NAME).get(self.id)
            if ds_temp != None:
                self.node = ds_temp

        try:
            self.node = globals.db.CreateDocument(EVENTS_COLLECTION_NAME, self.to_doc())
            if self.id == "":
                self.id = self.node["_key"]
        except Exception as ex:
            self.logger.error("Error creating or updating event on alerting database: %s", ex)
