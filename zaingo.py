import logging
import os
import signal
import sys
import threading
import time

from core import alerts
from core import globals
from core import node_communication as nc
from core import tasks
from core import zaingoDB as zdb
from core import zaingo_config as zc
from core import analysis as an
from core import plugin_loader as pl


class main():
    tasks_list = []
    logger = None
    running = True

    def load_configuration(self):
        configuration = zc.zaingoConfig()
        configuration.load_node_config()
        return configuration

    def initialize(self):
        globals.appPath = os.path.abspath(os.path.dirname(__file__))
        globals.configPath = globals.appPath + "/config"
        globals.configuration = self.load_configuration()
        format = '%(asctime)-15s - %(levelname)s - %(pathname)s - %(name)s - %(funcName)s - %(message)s'
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename = globals.configuration.logging_file, level = globals.configuration.logging_level, format = format)
        
        self.logger.debug("Creating task scheduler")
        globals.scheduler = tasks.Tasks()

        self.logger.debug("Initializating node mode architecture")
        if globals.configuration.node_mode == globals.configuration.NODE_MASTER or globals.configuration.node_mode == globals.configuration.NODE_FULL:
            self.initialize_master()
            self.create_master_tasks()
        if globals.configuration.node_mode == globals.configuration.NODE_ANALYSIS or globals.configuration.node_mode == globals.configuration.NODE_FULL:
            self.initialize_analytic()

    def initialize_master(self):
        self.logger.debug("Initializating system as MASTER")
        globals.db = self.initialize_database()
        globals.configuration.init_database()
        globals.configuration.set_core_configuration()
        globals.max_ttl = globals.configuration.max_ttl
        globals.master_thread = nc.Master(globals.configuration.master_host, globals.configuration.master_port)
        globals.master_thread.daemon = True
        globals.master_thread.start()
        time.sleep(3)

    def initialize_analytic(self):
        self.logger.debug("Initializating system as ANALYTIC")
        globals.analytic_thread = nc.Analytic(globals.configuration.master_host, globals.configuration.master_port)
        globals.analytic_thread.daemon = True
        globals.analytic_thread.start()
        time.sleep(3)
        if globals.db == None:
            try:
                response = globals.analytic_thread.send_order(nc.GET_DATABASE)
                globals.configuration.db_host=response[globals.configuration.CONFIG_MASTER_DATABASE_HOST]
                globals.configuration.db_port=response[globals.configuration.CONFIG_MASTER_DATABASE_PORT]
                globals.configuration.db_user=response[globals.configuration.CONFIG_MASTER_DATABASE_USER]
                globals.configuration.db_pass=response[globals.configuration.CONFIG_MASTER_DATABASE_PWD]
                globals.db=self.initialize_database()
            except Exception as e:
                self.logger.error("No database created. Error: %s", e)
                print ("Error creating database access. Please, review the configuration")
                self.exit()
        globals.configuration.set_core_configuration()
        

    def initialize_database(self):
        db=zdb.zaingoDB(globals.configuration.db_host, globals.configuration.db_port, globals.configuration.db_user, globals.configuration.db_pass)
        globals.events_collection_object=db.CreateCollection (globals.events_collection)
        globals.engines_collection_object=db.CreateCollection (globals.engines_collection)
        globals.events_edges_object=db.CreateCollection (globals.events_edges, True)
        globals.events_graph_object=db.DefineGraph(globals.events_graph, [globals.events_collection, globals.engines_collection], [globals.events_collection], [globals.engines_collection], globals.events_edges)

        db.CreateCollection(alerts.ALERTS_COLLECTION_NAME, truncate=False)
        db.CreateCollection(alerts.EVENTS_COLLECTION_NAME, truncate=False)
        db.CreateCollection(alerts.LOGIC_COLLECTION_NAME, truncate=False)
        db.CreateCollection(alerts.EDGES_COLLECTION_NAME, True, False)
        db.DefineGraph(alerts.GRAPH_NAME, [alerts.ALERTS_COLLECTION_NAME, alerts.EVENTS_COLLECTION_NAME,
                                           alerts.LOGIC_COLLECTION_NAME],
                                          [alerts.ALERTS_COLLECTION_NAME, alerts.EVENTS_COLLECTION_NAME,
                                           alerts.LOGIC_COLLECTION_NAME],
                                          [alerts.ALERTS_COLLECTION_NAME, alerts.LOGIC_COLLECTION_NAME,
                                           alerts.EVENTS_COLLECTION_NAME], alerts.EDGES_COLLECTION_NAME)
        return db

    # PLUGINS SECTION
    # ==============================
    def load_plugins(self):
        plugins=None
        plugins=pl.load_plugins()
        return plugins

    #INPUT PLUGINS SECTION
    #==============================
    def run_input_plugins(self, input_plugins, analysis):
        for key, value in input_plugins.items():
            if value.enable==True:
                self.logger.debug("Running plugin %s", key)
                if analysis!=None:
                    value.set_callback(analysis.analyze)
                try:
                    if globals.configuration.node_mode==globals.configuration.NODE_MASTER or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                        self.logger.debug("Running plugin %s as Master", key)
                        #t=threading.Thread(target=value.do_master, args=())
                        #t.start()
                        value.do_master()
                    if globals.configuration.node_mode==globals.configuration.NODE_ANALYSIS or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                        self.logger.debug("Running plugin %s as Analytic", key)
                        t=threading.Thread(target=value.do_analytic, args=())
                        t.start()

                    globals.inputs[value.name]=value
                except Exception as e:
                    self.logger.error("Unable to run plugin %s: %s", key, e)
            else:
                self.logger.debug("Plugin %s NOT enabled", key)

    #ANALYSIS ENGINES SECTION
    #===============================
    def initialize_analysis_engines(self, engines):
        for key, value in engines.items():
            self.logger.debug("Initializing analysis engine %s", key)
            try:
                self.logger.debug("Calling initialize on engine %s", key)
                if globals.configuration.node_mode==globals.configuration.NODE_MASTER or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                    self.logger.debug("Running plugin %s as Master", key)
                    value.do_master()
                if globals.configuration.node_mode==globals.configuration.NODE_ANALYSIS or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                    self.logger.debug("Running plugin %s as Analytic", key)
                    value.do_analytic()
                globals.engines[value.name]=value
            except Exception as e:
                self.logger.error("Unable to load engine initialize %s -- %s", key, e)
        self.logger.debug("All engines initialized")

    #OUTPUT PLUGINS SECTION
    #===============================
    def initialize_output_plugins(self, plugins):
        for key, value in plugins.items():
            self.logger.debug("Initializing output plugin %s", key)
            try:
                if globals.configuration.node_mode==globals.configuration.NODE_MASTER or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                    self.logger.debug("Running plugin %s as Master", key)
                    value.do_master()
                    globals.outputs[value.name]=value
            except Exception as e:
                self.logger.error("Unable to load plugins parameter %s -- %s. Error: %s", key, value, e)

        self.logger.debug("All plugins initialized")
        return plugins


    # TASK MANAGEMENT
    #=============================

    def create_master_tasks(self):
        ''' Initialize all the core tasks. By default they are defined at Tasks file '''            
        tk1=tasks.CleanOldEvents()
        tk1.create_task()


    def stop_tasks(self):
        '''Stop the global scheduler'''
        globals.scheduler.stop()

    #=============================

    def stop_system(self):
        if globals.analytic_thread!=None:
            self.logger.info('Stopping Analytic Thread')
            globals.analytic_thread.stop()
            globals.analytic_thread.join()
        if globals.master_thread!=None:
            self.logger.info('Stopping Master Thread')
            globals.master_thread.stop()
            globals.master_thread.join()

        for key, value in globals.inputs.items():
            self.logger.info('Stopping Input Plugin %s', key)
            value.stop()

    def exit(self):
        print ("Exiting zaingo...")
        self.stop_system()
        self.stop_tasks()
        self.logger.info('Exiting zaingo')
        print ("zaingo Out.")
        sys.exit(0)

    def signal_handler(self, signal, frame):
        print ("\n")
        self.running=False

    def run(self):
        self.initialize()

        self.logger.debug("Core System initialized")

        plugins=self.load_plugins()

        analysis_engines=plugins["engines"]
        input_plugins=plugins["inputs"]
        globals.outputs = plugins["outputs"]

        self.initialize_analysis_engines(analysis_engines)

        analysis=an.Analyze()#globals.engines)

        if globals.configuration.node_mode!=globals.configuration.NODE_MASTER and (input_plugins==None or len(input_plugins)==0):
            print ("*** You need to define at least ONE INPUT plugin ***")
            self.exit()

        self.run_input_plugins(input_plugins, analysis)

        #self.initialize_output_plugins(output_plugins)

        #signals
        signal.signal(signal.SIGINT, self.signal_handler)
        globals.scheduler.run()
        loops=0
        while self.running:
            time.sleep(0.5)
            loops=loops+1
            if loops>15:
                loops=0
                if globals.configuration.node_mode==globals.configuration.NODE_MASTER or globals.configuration.node_mode==globals.configuration.NODE_FULL:
                    globals.master_thread.ping()

        self.exit()

if __name__ == "__main__":
    main=main()
    main.run()