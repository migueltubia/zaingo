from apscheduler.schedulers.background import BackgroundScheduler
from core import globals
import logging
import datetime

class Tasks(object):
    """Class to manage the scheduler"""
    scheduler=None
    def __init__(self):
        self.logger = logging.getLogger('Tasks')
        self.scheduler = BackgroundScheduler()

    #: function to add a new jon/task to the scheduler
    #: Parameter: task, subclass of TasksInterface
    def add_job(self, task):
        if (isinstance(task,TasksInterface)) and task.executionTime>0:
            self.scheduler.add_job(task.execute_internal, 'interval', seconds=task.executionTime, id=task.name)
        else:
            self.logger.debug("The task must be subclass of TasksInterface")
    
    def remove_job(self, job):
        self.logger.debug("Removing Job")
        self.scheduler.remove_job(job)

    #: Function to start the scheduler
    def run(self):
        self.logger.debug("Running scheduler")
        self.scheduler.start()
        
    def stop(self):
        self.logger.debug("Stopping scheduler")
        if self.scheduler.running:
            self.scheduler.shutdown()


class TasksInterface(object):
    """Interface, base class, for all Taks in zaingo"""
    #: Task's name
    #: From configuration file, we can access to this task usin the name
    name = ""
    
    #: Task's description
    description = ""
    
    #: Task's execution Time, in seconds.
    #: When the task must be executed. Each task can have it's own execution time.
    executionTime = 0
    
    plugin_name = ""
    
    plugin_type = ""

    last_execution = datetime.datetime.now().timestamp()

    def __init__(self):
        self.logger = logging.getLogger('TasksInterface')

    #: configuration: parameters json for the task. We get the execution time and other parameters
    def set_configuration(self, configuration):
        self.logger.debug("%s: set parameters %s", self.name, configuration)
        for key, value in configuration.items():
            setattr(self, key, value)
    
    def load_configuration(self):
        config=globals.configuration.load_tasks_configuration(self.plugin_type, self.plugin_name, self.name)
        if config==None:
            self.init_configuration()
        else:
            self.set_configuration(config)
    
    def init_configuration(self):
        configuration={}
        configuration["executionTime"] = self.executionTime
        globals.configuration.save_task_configuration(plugin_type = self.plugin_type, plugin_name = self.plugin_name, task_name = self.name, configuration = configuration)
    
    def create_task(self):
        self.load_configuration()
        globals.scheduler.add_job(self)

    def execute_internal(self):
        self.last_execution = datetime.datetime.now().timestamp()
        self.execute()

    def execute(self):
        pass

class CleanOldEvents(TasksInterface):
    def __init__(self):
        super(CleanOldEvents, self).__init__()
        self.logger = logging.getLogger('zaingo:CleanOldEvents')
        self.name="CleanOldEvents"
        self.description="Clean old events"
        self.executionTime=30
        self.plugin_name="zaingo"

    def execute(self):
        self.logger.debug("Executing zaingo:%s task", self.name)
        now=(datetime.datetime.now()-datetime.timedelta(seconds = 60)).timestamp()
        qs="FOR e in " + globals.events_collection + " FILTER e._id not in (FOR m in " + globals.events_edges + " FILTER used = True RETURN m._from) AND e.rule_updated <=" + str(now) + " REMOVE { _key: e._key } IN " + globals.events_collection
        try:
            globals.db.ExecuteQuery(qs)
            self.logger.debug("Executed markup task OK")
        except Exception as error:
            self.logger.error("Error executing markup task: %s -- %s ", error, qs)