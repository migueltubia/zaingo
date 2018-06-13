import logging
from abc import ABCMeta, abstractmethod
from . import main as sa
import datetime

class BaseObject (metaclass = ABCMeta):

    internal_id = ""
    name = ""
    description = ""
    condition = None
    internal_condition = None

    def __init__(self):
        self.logger = logging.getLogger('BaseObject')
        self.internal_id = ""
        self.name = ""
        self.description = ""
        self.condition = None
        self.internal_condition = None

    def evaluate_condition(self, data):
        if self.condition == None:
            return self.internal_condition.calculate(data)
        else:
            return self.condition.calculate(data)

    @abstractmethod
    def analyze_event(self, data):
        pass

class BaseObjectData ():

    node = None
    config_object = None
    created = None
    modified = None
    status = None
    internal_id = ""

    def __init__(self):
        #super(BaseObjectData, self).__init__()
        self.logger = logging.getLogger('BaseObjectData')
        self.node = None
        self.config_object = None
        self.created = datetime.datetime.now().timestamp()
        self.modified = datetime.datetime.now().timestamp()
        self.status = None
        self.internal_id = ""

    def from_json (self, data):
        self.internal_id = data["internal_id"] if "internal_id" in data.keys() else self.internal_id
        self.created = data["created"] if "created" in data.keys() else datetime.datetime.now().timestamp()
        self.modified = data["modified"] if "modified" in data.keys() else datetime.datetime.now().timestamp()
        self.config_object = sa.ScenarioAnalysis.objects[self.internal_id] if self.internal_id != "" else None
        self.node = data

    def to_json(self):
        data = {}

        data["internal_id"] = self.internal_id
        data["created"] = self.created
        data["modified"] = self.modified
        data["status"] = self.status.value
        if self.node != None:
            data["_id"] = self.node["_id"]
            data["_key"] = self.node["_key"]

        return data