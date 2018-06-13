import logging
import configparser
from core import globals
import sys

class zaingoConfig:

    config_file= "zaingo.conf"
    CONFIG_COLLECTION_NAME="configuration_collection"
    CONFIG_COLLECTION_FIELD_TYPE="type"
    CONFIG_COLLECTION_FIELD_NAME="name"
    CONFIG_COLLECTION_FIELD_TASK="tasks"
    CONFIG_COLLECTION_FIELD_DATA="configuration"
    CONFIG_COLLECTION_FIELD_DATA_DEFAULT = "default"

    #All basic sections
    CONFIG_GENERAL="general"
    CONFIG_LOGGING="logging"
    CONFIG_MASTER="master"
    CONFIG_ANALYTIC="analytic"

    CONFIG_GENERAL_MODE="general.mode"

    CONFIG_LOGGING_FILE="logging.file"
    CONFIG_LOGGING_LEVEL="logging.level"
    
    CONFIG_MASTER_HOST="master.host"
    CONFIG_MASTER_PORT="master.port"
    CONFIG_MASTER_DATABASE_HOST="master.db_host"
    CONFIG_MASTER_DATABASE_PORT="master.db_port"
    CONFIG_MASTER_DATABASE_USER="master.db_user"
    CONFIG_MASTER_DATABASE_PWD="master.db_pass"

    CONFIG_ANALYTIC_NAME="analytic.name"

    
    CONFIG_ANALYTIC_CORE="zaingo"
    CONFIG_ANALYTIC_MAXTTL="max_ttl"


    NODE_MASTER="master"
    NODE_ANALYSIS="analysis"
    NODE_FULL="full"

    LOG_LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

    node_mode=""
    
    logging_file="zaingo.log"
    logging_level="debug"

    master_host="localhost"
    master_port=5001
    
    db_host="localhost"
    db_port=8529
    db_user="root"
    db_pass="root"

    name="node"

    #core configuration
    max_ttl=60
    
    def __init__(self):
        self.config_file=globals.configPath+"/zaingo.conf"
        self.logging_file=globals.appPath+"/zaingo.log"

    '''
    Function: load_node_config
    Loads all the configuration for this node
    '''
    def load_node_config(self):
        settings = configparser.ConfigParser()
        settings._interpolation = configparser.ExtendedInterpolation()
        settings.read(self.config_file)
        #Getting logging configuration
        #self.logging_file=settings.get(self.CONFIG_LOGGING, self.CONFIG_LOGGING_FILE)
        if self.CONFIG_LOGGING in settings:
            self.logging_file= settings.get(self.CONFIG_LOGGING, self.CONFIG_LOGGING_FILE) if self.CONFIG_LOGGING_FILE in settings[self.CONFIG_LOGGING] else self.logging_file
            level= settings.get(self.CONFIG_LOGGING, self.CONFIG_LOGGING_LEVEL) if self.CONFIG_LOGGING_LEVEL in settings[self.CONFIG_LOGGING] else "debug"
            self.logging_level = self.LOG_LEVELS.get(level.lower(), logging.NOTSET)
        #Getting node mode
        self.node_mode = settings.get(self.CONFIG_GENERAL, self.CONFIG_GENERAL_MODE)
        self.node_mode=self.node_mode.lower()
        if self.node_mode==self.NODE_MASTER or self.node_mode==self.NODE_FULL:
            self.master_host = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_HOST)
            self.master_port = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_PORT)
            self.db_host = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_DATABASE_HOST)
            self.db_port = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_DATABASE_PORT)
            self.db_user = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_DATABASE_USER)
            self.db_pass = settings.get(self.CONFIG_MASTER, self.CONFIG_MASTER_DATABASE_PWD)
        if self.node_mode==self.NODE_ANALYSIS or self.node_mode==self.NODE_FULL:
            self.master_host = settings.get(self.CONFIG_ANALYTIC, self.CONFIG_MASTER_HOST)
            self.master_port = settings.get(self.CONFIG_ANALYTIC, self.CONFIG_MASTER_PORT)
            self.name = settings.get(self.CONFIG_ANALYTIC, self.CONFIG_ANALYTIC_NAME)
        if self.node_mode=="":
            print ("No mode configured. Exiting. Please review your configuration")
            sys.exit("zaingo. Fatal Error. End of program")
            
    def init_database(self):
        globals.db.CreateCollection(self.CONFIG_COLLECTION_NAME, isEdge=False, truncate=False)


    def load_configuration(self, plugin_type, plugin_name, config_type=None):
        data={}
        if globals.db==None:
            return None

        filters={}
        filters[self.CONFIG_COLLECTION_FIELD_NAME]=plugin_name
        if plugin_type!="" and plugin_type!=None:
            filters[self.CONFIG_COLLECTION_FIELD_TYPE]=plugin_type
        response=globals.db.SearchDocuments(self.CONFIG_COLLECTION_NAME, filters)
        if response!=None and response.count()==1:
            data=response.next()
            if config_type!=None:
                if config_type in data:
                    data=data[config_type]
                else:
                    data=None
        elif response==None or response.count()==0:
            data=None
                        
        return data
    
    def save_configuration(self, configuration, plugin_type, plugin_name):
        if globals.db==None:
            return
        document=configuration
        filters={}
        #Set the filters
        filters[self.CONFIG_COLLECTION_FIELD_NAME]=plugin_name
        if plugin_type!="" and plugin_type!=None:
            filters[self.CONFIG_COLLECTION_FIELD_TYPE]=plugin_type
        
        #Create the new configuration
        document[self.CONFIG_COLLECTION_FIELD_NAME]=plugin_name
        if plugin_type!="" and plugin_type!=None:
            document[self.CONFIG_COLLECTION_FIELD_TYPE]=plugin_type
        
        #Execute the upsert
        globals.db.UpsertDocument(col=self.CONFIG_COLLECTION_NAME, filters=filters, document=document, replace=False)

    def load_tasks_configuration(self, plugin_type, plugin_name, task_name=None):
        configuration=self.load_configuration(plugin_type=plugin_type, plugin_name=plugin_name, config_type=self.CONFIG_COLLECTION_FIELD_TASK)
        if configuration==None:
            return configuration

        if task_name!=None:
            if task_name in configuration:
                configuration=configuration[task_name]
            else:
                configuration=None
        return configuration
    
    def save_task_configuration(self, plugin_type, plugin_name, task_name, configuration):
        if globals.db==None:
            return

        document=self.load_configuration(plugin_type=plugin_type, plugin_name=plugin_name, config_type=None)
        if document==None:
            self.save_configuration({}, plugin_type, plugin_name)
            document={}
        
        if self.CONFIG_COLLECTION_FIELD_TASK not in document:
            document[self.CONFIG_COLLECTION_FIELD_TASK]={}
        
        if task_name not in document[self.CONFIG_COLLECTION_FIELD_TASK]:
            document[self.CONFIG_COLLECTION_FIELD_TASK][task_name]={}
        
        document[self.CONFIG_COLLECTION_FIELD_TASK][task_name]=configuration

        self.save_configuration(document, plugin_type, plugin_name)


    def set_core_configuration(self):
        configuration={}
        configuration=self.load_configuration(plugin_type="", plugin_name=self.CONFIG_ANALYTIC_CORE, config_type=self.CONFIG_COLLECTION_FIELD_DATA)
        if configuration!=None and len(configuration)>0:
            for key, value in configuration.items():
                setattr(self, key, value)
        else:
            self.init_plugin_configuration(plugin_type="", plugin_name=self.CONFIG_ANALYTIC_CORE, config_type=self.CONFIG_COLLECTION_FIELD_DATA)
                
    def init_plugin_configuration(self, plugin_type, plugin_name, config_type=None):
        configuration={}

        configuration[self.CONFIG_COLLECTION_FIELD_DATA]={}
        configuration[self.CONFIG_COLLECTION_FIELD_TASK]={}
        configuration[self.CONFIG_COLLECTION_FIELD_DATA][self.CONFIG_COLLECTION_FIELD_DATA_DEFAULT]={}
        
        #for k, v in configuration.items():
        #    configuration[k]={}
        
        self.save_configuration(configuration=configuration, plugin_type=plugin_type, plugin_name=plugin_name)
        return configuration