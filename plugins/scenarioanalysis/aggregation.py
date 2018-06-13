import logging
from enum import Enum

class AggregationStatus(Enum):
    STATUS_CLOSED = "CLOSED"
    STATUS_OPEN = "OPEN"

class Aggregation:

    code = 0
    fields = []
    number = 0
    time = 0
    threat = 0

    def __init__(self):
        self.logger = logging.getLogger('Aggregation')
        self.code = 0
        self.fields = []
        self.number = 0
        self.time = 0
        self.threat = 0

    @staticmethod
    def create_aggregation(o):
        aggregation = Aggregation()

        aggregation.code = o["code"]
        aggregation.number= o["number"] if "number" in o.keys() else 0
        aggregation.time = o["time"] if "time" in o.keys() else 0
        aggregation.threat= o["threat"] if "threat" in o.keys() else 0
        aggregation.fields= o["fields"] if "fields" in o.keys() else []

        return aggregation

    def get_fields(self, data):
        self.logger.debug("Setting aggregation fields")
        values = {}
        if self.fields != []:
            values = {}
            for key in self.fields:
                self.logger.debug("Setting field %s", key)
                if data.has_attribute(key):
                    values[key] = data.get_attribute(key)
                    self.logger.debug("Key found")
                else:
                    self.logger.debug("Data has not the attribute")
        return values

class AggregationData:

    code = 0
    aggregation = None
    times = 0
    first_time = None
    last_time = None
    # All values seen as arrays of json objects
    field_values = {}
    status = AggregationStatus.STATUS_OPEN

    def __init__(self):
        self.logger = logging.getLogger('AggregationData')
        self.aggregation = None
        self.times = 0
        self.first_time = None
        self.last_time = None
        self.field_values = {}
        self.code = 0
        self.status = AggregationStatus.STATUS_OPEN

    def create(self, agg, data):
        self.aggregation = agg
        self.code = agg.code
        self.first_time = data.control_time
        self.last_time = data.control_time
        self.field_values = agg.get_fields(data)

    def from_json (self, data):
        self.logger.debug("Creating object from json")
        self.times = data["times"]
        self.first_time = data["first_time"]
        self.last_time = data["last_time"]
        self.field_values = data["field_values"]
        self.code= data["code"]
        self.status = AggregationStatus(data["status"]) if "status" in data.keys() else AggregationStatus.STATUS_OPEN

    def to_json (self):
        self.logger.debug("Creating json from object")
        json = {}
        json["times"] = self.times
        json["first_time"] = self.first_time
        json["last_time"] = self.last_time
        json["field_values"] = self.field_values
        json["code"] = self.code
        json["status"] = self.status.value
        return json

    def do (self, data):
        threat_previous = 0
        threat = 0
        if self.aggregation.number > 0:
            threat_previous = self.aggregation.threat * int(self.times / self.aggregation.number)
        if not (self.status == AggregationStatus.STATUS_CLOSED or (data.control_time - self.last_time > self.aggregation.time)):
            self.times += 1
            self.last_time = data.control_time
        else:
            self.logger.debug("Aggregation timed out")
            self.status = AggregationStatus.STATUS_CLOSED
        if self.aggregation.number > 0:
            threat = self.aggregation.threat * int(self.times / self.aggregation.number)
            self.logger.debug("Threat is %s", threat)
        else:
            self.logger.debug("Not calculating, number is 0")
        return threat-threat_previous
