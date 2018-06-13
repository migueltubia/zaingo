#def init():
#: Global variables, common to ALL zaingo
#: DO NOT TOUCH if you are not sure about what you are doing
global db, inputs, outputs, engines, appPath, configPath, scheduler, max_ttl, events_collection, engines_collection, events_collection_object, engines_collection_object, events_edges, events_edges_object, events_graph, events_graph_object, alert, configuration, master_thread, analytic_thread
db = None
inputs={}
outputs={}
engines={}
alert=None
appPath=""
configPath=""
scheduler=None
max_ttl=90
master_thread=None
analytic_thread=None
#Variables for create generic database objects
events_collection="events_collection"
engines_collection="engines_collection"
events_collection_object=None
engines_collection_object=None
events_edges="events_edges"
events_edges_object=None
events_graph="events_graph"
events_graph_object=None
configuration=None