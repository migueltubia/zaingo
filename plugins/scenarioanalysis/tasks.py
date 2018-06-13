import logging
from core import tasks as tk
from core import globals
from . import main as sa
from . import control as control
from . import aggregation as agg
from . import level
from . import scenario

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

    '''
        Close the levels that:
        - Are open (status)
        - All the controls are closed
    '''
    def execute_levels(self):
        self.logger.debug("Executing Level stage in :%s task", self.name)
        qs = "FOR level in " + sa.ScenarioAnalysis.LEVEL_COLLECTION + \
        " FILTER LENGTH(" + \
        " FOR v in 0..1 OUTBOUND level "+sa.ScenarioAnalysis.SCENARIO_EDGES + \
        " FILTER IS_SAME_COLLECTION('" + sa.ScenarioAnalysis.CONTROL_COLLECTION + "', v)" + \
        " AND v.status == '" + control.ControlStatus.STATUS_OPEN.value + "'" + \
        " RETURN v) == 0" + \
        " UPDATE level WITH {status: '" + level.LevelStatus.STATUS_CLOSE.value + "'} IN " + sa.ScenarioAnalysis.LEVEL_COLLECTION
        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed markup task OK")
        except Exception as error:
            self.logger.error("Error executing markup task: %s -- %s ", error, qs)

    '''
        Close the scenarios that:
        - Are opened (status)
        - All the levels are closed
    '''
    def execute_scenarios(self):
        self.logger.debug("Executing Scenario stage in :%s task", self.name)
        qs = "FOR scenario in " + sa.ScenarioAnalysis.SCENARIO_COLLECTION + \
        " FILTER LENGTH(" + \
        " FOR v in 0..1 OUTBOUND scenario "+sa.ScenarioAnalysis.SCENARIO_EDGES + \
        " FILTER IS_SAME_COLLECTION('" + sa.ScenarioAnalysis.LEVEL_COLLECTION + "', v)" + \
        " AND v.status == '" + level.LevelStatus.STATUS_AWAKE.value + "'" + \
        " RETURN v) == 0" + \
        " UPDATE scenario WITH {status: '" + scenario.ScenarioStatus.STATUS_CLOSE.value + "'} IN " + sa.ScenarioAnalysis.SCENARIO_COLLECTION
        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed markup task OK")
        except Exception as error:
            self.logger.error("Error executing markup task: %s -- %s ", error, qs)

class CalculateThreat(tk.TasksInterface):

    def __init__(self):
        super(CalculateThreat, self).__init__()
        self.logger = logging.getLogger('ScenarioAnalysis:CalculateThreat')
        self.name = "CalculateThreat"
        self.description = "Calculate threat for controls, levels and scenarios"
        self.executionTime = 60
        self.plugin_name = "ScenarioAnalysis"
        self.plugin_type = "engine"

    def execute(self):
        self.logger.debug("Executing ScenarioAnalysis:%s task", self.name)
        self.execute_levels()
        self.execute_scenarios()

    def execute_levels(self):
        self.logger.debug("Executing Level stage in :%s task", self.name)

        qs="FOR level in " + sa.ScenarioAnalysis.LEVEL_COLLECTION + \
        " LET datos=(FOR v in 0..1 OUTBOUND level " + sa.ScenarioAnalysis.SCENARIO_EDGES + \
        " FILTER IS_SAME_COLLECTION('" + sa.ScenarioAnalysis.CONTROL_COLLECTION + "', v)" + \
        " COLLECT tt = level" + \
        " AGGREGATE threat = SUM(v.threat)" + \
        " RETURN threat)" + \
        " UPDATE level WITH {threat: (first(datos) == null ? 0 : first(datos))} IN " + sa.ScenarioAnalysis.LEVEL_COLLECTION

        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed calculation task OK")
        except Exception as error:
            self.logger.error("Error executing calculation task: %s -- %s ", error, qs)

    def execute_scenarios(self):
        self.logger.debug("Executing Scenario stage in :%s task", self.name)

        qs = "FOR scenario in "+ sa.ScenarioAnalysis.SCENARIO_COLLECTION + \
        " LET datos=(FOR v in OUTBOUND scenario "+ sa.ScenarioAnalysis.SCENARIO_EDGES + \
        " FOR level in "+ sa.ScenarioAnalysis.LEVEL_CONFIG_COLLECTION + \
        " FILTER IS_SAME_COLLECTION('" + sa.ScenarioAnalysis.LEVEL_COLLECTION + "', v)" + \
        " AND v.internal_id == level._id" + \
        " COLLECT tt = scenario, ll = level" + \
        " AGGREGATE threat = SUM(v.threat * (level.threat_to_scenario/100))" + \
        " RETURN threat)" + \
        " UPDATE scenario WITH {threat: (first(datos) == null ? 0 : first(datos))} IN "+ sa.ScenarioAnalysis.SCENARIO_COLLECTION

        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed calculation task OK")
        except Exception as error:
            self.logger.error("Error executing calculation task: %s -- %s ", error, qs)

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