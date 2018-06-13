import logging
import datetime
import asyncio
from plugins import edict
from core import globals

class Analyze(object):

    def __init__(self):
        self.logger = logging.getLogger('Analyze')
        self.logger.debug("Created analysis object")

    def analyze(self, data):
        self.logger.debug("New data %s", data)
        data_shield={}
        data_shield["source"]=data
        ed=edict.edict(data_shield)
        node=self.save_event(data_shield)
        ed._id=node["_id"]
        for key, value in globals.engines.items():
            loop=asyncio.new_event_loop()
            try:
                loop.run_until_complete(value.execute_internal(ed))
            except Exception as error:
                self.logger.error("Error executing engine %s: %s", key, error)
            loop.close()

    #def add_engine(self, engine):
    #    self.engines[engine.name]=engine
            
    def save_event (self, data):
        node=None
        rule_updated=datetime.datetime.now().timestamp()
        data["rule_updated"]=rule_updated
        try:
            node=globals.db.CreateDocument(globals.events_collection_object, data)
        except Exception as error:
            self.logger.error("Error creating node: %s", error)
            node=None
        return node