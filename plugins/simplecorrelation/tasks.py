import logging
from core import tasks as tk
from core import globals
from . import main as sa
from . import control as control
from . import aggregation as agg

class MarkAsInactive(tk.TasksInterface):

    def __init__(self):
        super(MarkAsInactive, self).__init__()
        self.logger = logging.getLogger('ScenarioAnalysis:MarkAsInactive')
        self.name = "MarkAsInactive"
        self.description = "Set as Inactive elements with TTL exceeded"
        self.executionTime = 60
        self.plugin_name = "ScenarioAnalysis"
        self.plugin_type = "engine"

    def execute(self):
        self.logger.debug("Executing ScenarioAnalysis:%s task", self.name)
        self.execute_aggregations()
        self.execute_controls()
        self.execute_levels()
        self.execute_scenarios()

    '''
    Close the control's aggregations that:
    - Status is Open.
    - the last time of update + time_to_life is <= now
    '''
    def execute_aggregations(self):
        self.logger.debug("Executing Aggregations stage in :%s task", self.name)
        qs = "FOR cc in " + sa.ScenarioAnalysis.CONTROL_COLLECTION + \
        " FOR ccc in " + sa.ScenarioAnalysis.CONTROL_CONFIG_COLLECTION + \
        " FILTER cc.internal_id == ccc._id" + \
        " LET alteredList = (" + \
        " FOR cca in cc.aggregations" + \
        " FOR ccca in ccc.aggregations" + \
        " FILTER cca.code == ccca.code AND cca.status == '" + agg.AggregationStatus.STATUS_OPEN.value + "' " + \
        " LET new_item = ( ! (cca.last_time + ccca.time <= DATE_NOW()/1000) ? cca : MERGE(cca, { status: '" + agg.AggregationStatus.STATUS_CLOSED.value + "' }))" + \
        " RETURN new_item" + \
        ")" + \
        " UPDATE cc WITH { aggregations:  alteredList } IN " + sa.ScenarioAnalysis.CONTROL_COLLECTION
        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed markup task OK")
        except Exception as error:
            self.logger.error("Error executing markup task: %s -- %s ", error, qs)

    '''
    Close the controls that:
    - Are open (status)
    - The last time modified plus the ttl is <= now
    - OR the time created plus max_time_to_life is <= now
    - all the aggregations are closed
    '''
    def execute_controls(self):
        self.logger.debug("Executing Control stage in :%s task", self.name)
        qs = "FOR cc in " + sa.ScenarioAnalysis.CONTROL_COLLECTION + \
        " FOR ccc in " + sa.ScenarioAnalysis.CONTROL_CONFIG_COLLECTION + \
        " FILTER cc.internal_id == ccc._id" + \
        " AND (" + \
        " (NOT ('" + control.ControlStatus.STATUS_CLOSED.value + "' IN cc.aggregations[*].status) AND " + \
        " cc.modified + ccc.time_to_life <= DATE_NOW()/1000)" + \
        " OR cc.created + ccc.max_time <= DATE_NOW()/1000)" + \
        " UPDATE cc WITH {status: '" + control.ControlStatus.STATUS_CLOSED.value + "'} IN " + sa.ScenarioAnalysis.CONTROL_COLLECTION
        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed markup task OK")
        except Exception as error:
            self.logger.error("Error executing markup task: %s -- %s ", error, qs)


class ManageScenarios(tk.TasksInterface):

    def __init__(self):
        super(ManageScenarios, self).__init__()
        self.logger = logging.getLogger('ScenarioAnalysis:ManageScenarios')
        self.name = "ManageScenarios"
        self.description = "Manage the scenarios for alerting and removing them"
        self.executionTime = 60
        self.plugin_name = "ScenarioAnalysis"
        self.plugin_type = "engine"

    def execute(self):
        self.execute_alerting()
        self.execute_remove()

    def execute_alerting(self):
        self.logger.debug("Executing ScenarioAnalysis:%s task", self.name)

        qs = "FOR scenario in " + sa.ScenarioAnalysis.SCENARIO_COLLECTION + \
        " FOR scenario_c in " + sa.ScenarioAnalysis.SCENARIO_CONFIG_COLLECTION + \
        " FILTER scenario.internal_id == scenario_c._id" + \
        " AND scenario.threat >= scenario_c.threat_level" + \
        " AND scenario.zaingo_alert_id == ''" + \
        " RETURN scenario"

        ds = None

        try:
            ds = globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed alerting query OK")
        except Exception as error:
            self.logger.error("Error executing alerting task: %s -- %s ", error, qs)

        if ds != None:
            for node in ds:
                scenario_id = node["_key"]
                alert = globals.engines["ScenarioAnalysis"].new_alert(alert = scenario_id)

        self.logger.debug("Executed alerting task OK")

    def execute_remove(self):
        self.logger.debug("Executing ScenarioAnalysis:%s task", self.name)

        qs_v = "FOR scenario in " + sa.ScenarioAnalysis.SCENARIO_COLLECTION + \
            " FILTER scenario.status == '" + scenario.ScenarioStatus.STATUS_CLOSE.value + "'" +\
            " FOR v IN 0..20 OUTBOUND scenario " + \
            " GRAPH '" + sa.ScenarioAnalysis.SCENARIO_GRAPH + "'" + \
            " RETURN v"

        qs_e = "FOR scenario in " + sa.ScenarioAnalysis.SCENARIO_COLLECTION + \
               " FILTER scenario.status == '" + scenario.ScenarioStatus.STATUS_CLOSE.value + "'" + \
               " FOR v,e,p IN 0..20 OUTBOUND scenario " + \
               " GRAPH '" + sa.ScenarioAnalysis.SCENARIO_GRAPH + "'" + \
               " RETURN e"

        vertices = None
        edges = None

        try:
            vertices = globals.db.ExecuteQuery(qs_v)
            edges = globals.db.ExecuteQuery(qs_e)
            self.logger.debug("Executed remove query OK")
        except Exception as error:
            self.logger.error("Error executing remove task: %s -- %s ", error)

        #if vertices != None:
            #for node in vertices:
                #if sa.ScenarioAnalysis.SCENARIO_COLLECTION in node["_id"] and node["zaingo_alert_id"] != "":
                    #globals.alert.get_alert(node["zaingo_alert_id"])
                #globals.db.remove_document(node)

        if edges != None:
            for node in edges:
                if node != None:
                    globals.db.remove_document(node)

        self.logger.debug("Executed remove task OK")