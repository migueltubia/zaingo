import logging
from arango import ArangoClient

class zaingoDB(object):
    #host="localhost"
    #port=8529
    database=None

    belongs="belongs_to"

    #: Initialize the connection to the DB.
    #: Parameters:
    #: host. Default: localhost
    #: port. Default: 8529
    #: user. Default: root
    #: password. Default: blank (no password)
    def __init__(self, host="localhost", port="8529", user="root", password="root"):
        self.logger = logging.getLogger('zaingoDB')
        self.database=None
        connection=None
        try:
            connection=ArangoClient(host=host, port=port, username=user, password=password)
        except Exception as e:
            self.logger.error("Error connecting to db: %s", e)
        if connection!=None:
            self.CreateDB(connection)

    #: Function to create the database 'zaingo' if it does not exist
    def CreateDB(self, conn):
        name="zaingo"
        self.logger.debug("Creating database")
        try:
            if name not in conn.databases(user_only=True):
                self.database=conn.create_database(name)
            else:
                self.database=conn.database(name)
        except Exception as e:
                self.logger.error("Error creating db: %s", e)


    #: Function to create one collection.
    #: Parameters:
    #: isEdge. Default: False. To set if the collection contains edge information
    #: truncate. Default: True. If the collection exists, should we delete all the data?
    def CreateCollection(self, collection, isEdge=False, truncate=False):
        col=None
        self.logger.debug("Creating collection %s", collection)

        cols=self.database.collections()

        if any(c["name"]==collection for c in cols):
            col=self.database.collection(collection)
            if truncate:
                col.truncate()
        else:
            try:
                col=self.database.create_collection(collection, edge=isEdge)
            except Exception as e:
                self.logger.error("Error creating collection: %s. %s", collection, e)

        return col

    #: Function to create a graph
    #: Parameters:
    #: name. Graph's name
    #: vertex. Array of vertex collecton
    #: _from. Array of all the vertex collecton with starting relations
    #: _to. Array of all the vertex collecton with ending relations
    #: edges. Name of the edges collection
    def DefineGraph(self, name, vertex, _from, _to, edges):
        graph = None
        self.logger.debug("Creating graph %s", name)

        graphs = self.database.graphs()

        if any(g["name"] == name for g in graphs):
            graph = self.database.graph(name)
        else:
            try:
                #if graph does not exist:
                graph = self.database.create_graph(name)
                for v in vertex:
                    graph.create_vertex_collection(v)
                graph.create_edge_definition(
                    name = edges,
                    from_collections = _from,
                    to_collections = _to,
                )
            except Exception as e:
                self.logger.error("Error creating graph: %s", e)

        return graph
            
    def CreateDocument(self, col, doc):
        document = None
        if type(col) is str:
            try:
                col = self.database.collection(col)
            except:
                self.logger.error("Collection %s NOT found", col)
                return None

        self.logger.debug("Creating document %s on collection %s", str(doc), col.name)
        if "_key" in doc:
            try :
                document = col.get(doc["_key"])
            except Exception as e:
                self.logger.debug("Document doesn't exist")
                document = None
        
        try:
            if document == None:
                document = col.insert(doc)
            else:
                document = col.update(doc)
        except Exception as e:
            self.logger.error("Error creating document: %s", e)
        return document

    def GetDocument(self, id):
        node=None
        self.logger.debug("Searching for document %s", id)
        try:
            data_array = id.split("/")
            _col = data_array[0]
            _id = data_array[1]
            col = self.database.collection(_col)
            node = col.get(_id)
        except Exception as e:
            self.logger.error("Document not found %s", e)
        return node

    def SearchDocuments(self, col, filters = {}):
        cursor = None
        self.logger.debug("Searching documents on collection %s with filters %s", col, filters)
        if type(col) is str:
            try:
                col = self.database.collection(col)
            except:
                self.logger.error("Collection %s NOT found", col)
                return None
        try:
            cursor = col.find(filters = filters)
        except Exception as e:
            self.logger.error("Error searching documents %s", e)
            cursor = None

        return cursor
    
    def UpsertDocument(self, col, filters, document, replace=True):
        self.logger.debug("Upsert document %s with filters %s on collection %s", document, filters, col)
        if type(col) is str:
            try:
                col = self.database.collection(col)
            except:
                self.logger.error("Collection %s NOT found", col)
        old_doc = self.SearchDocuments(col, filters)
        if old_doc.count() == 0:
            self.CreateDocument(col, document)
        else:
            try:
                if replace:
                    col.replace_match(filters, document)
                else:
                    col.update_match(filters, document)
            except Exception as e:
                self.logger.error("Error replacing documents %s", e)

    def CreateEdge(self, graph, _from, _to, edge, data, label):
        node = None

        if type(graph) is str:
            try:
                graph = self.database.graph(graph)
            except Exception as e:
                self.logger.error("Graph %s NOT found: %s", graph, e)
                return None

        self.logger.debug("Creating Edge from %s to %s on graph %s and edge collection %s", _from, _to, graph.name, edge)
        try:
            edge_node = graph.edge_collection(edge)
            node = None
            document = {'_from': _from, '_to': _to}
            nodes = self.SearchDocuments(edge_node, document)
            if nodes != None and nodes.count() > 0:
                node = nodes.next()
            else:
                node = edge_node.insert({'_from': _from, '_to': _to})
            self.logger.debug("Created node %s", node)
            node["type"]=label
            if data != None:
                for k, v in data.items():
                    node[k] = v
            edge_node.update(node)
        except Exception as e:
            self.logger.error("Error creating edge: %s", e)
        return node
        
    def RemoveVertex(self, graph, node):
        self.logger.debug("Removing Vertex %s from graph %s", node, graph.name)
        try:
            graph.delete_vertex_collection(node)
        except Exception as e:
            self.logger.error("Error deleting node: %s", e)
        
    def ExecuteQuery(self, query):
        result=None
        self.logger.debug("Executing query %s", query)
        try:
            result=self.database.aql.execute(query, count=True)
        except Exception as e:
            self.logger.error("Error executing query: %s", e)
        return result
        
    def CreateFunction(self, name, body):
        if name in self.database.aql.functions():
            self.database.aql.delete_function(name)

        self.database.aql.create_function(name, body)

    def Graph_Go_Forward (self, graph, start, vertex_filters = "", depth = None, direction="outbound"):
        results=None

        if type(graph) is str:
            try:
                graph=self.database.graph(graph)
            except Exception as e:
                self.logger.error("Graph %s NOT found: %s", graph, e)
                return None

        self.logger.debug("Creating traversal search on graph %s from %s ", str(graph), start)

        filter_func = ""

        if vertex_filters != "":
            filter_func = "if (" +vertex_filters + ") { return \"exclude\";}return;"

        #print (filter_func)

        try:
            results=graph.traverse(start_vertex=start, direction = direction, item_order="forward", filter_func=filter_func )
        except Exception as e:
            self.logger.error("Error executing traverse: %s", e)

        return results

    def get_collection(self, name):
        return self.database.collection(name)

    def get_graph(self, name):
        return self.database.graph(name)

    def remove_document(self, document):
        id=document["_id"]
        self.logger.debug("Searching for document %s", id)
        try:
            data_array = id.split("/")
            _col = data_array[0]
            col = self.database.collection(_col)
            col.delete(document)
            self.logger.debug("Document remove: %s", document)
        except Exception as e:
            self.logger.error("Document not found %s", e)