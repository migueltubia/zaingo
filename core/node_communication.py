from core import globals
import logging
import socket
import threading
import json
import datetime

# Database collections
NODES_COLLECTION = "nodes_collection"

# Orders to/from Clients
GET_DATABASE = "get_database"
GET_NAME = "get_name"
GET_ENGINES = "get_engines"
GET_INPUTS = "get_inputs"
NEW_ALERT = "new_alert"
# get_configuration order
GET_CONFIGURATION = "get_configuration"

# parameters for orders
PARAMETER_PLUGIN_TYPE = "plugin_type"
PARAMETER_PLUGIN_NAME = "plugin_name"
PARAMETER_ALERT = "parameter"

# ping order
PING = "ping"
PONG = "pong"

EXIT = "exit"

# Othet data
TIMEOUT = 30


class Master(threading.Thread):
    # host="localhost"
    # port=5001
    server = None
    to_stop = False
    clients = {}

    #: Initialize the matser.
    #: Parameters:
    #: host. Default: localhost
    #: port. Default: 5001
    def __init__(self, host="localhost", port=5001):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('Master')
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((host, int(port)))
        globals.db.CreateCollection(NODES_COLLECTION)

    def run(self):
        self.startMaster()

    def stop(self):
        self.stopMaster()

    def startMaster(self):
        self.logger.debug("Starting Master Networking Server")
        self.server.listen(5)
        while not self.to_stop:
            try:
                client, address = self.server.accept()
                client.settimeout(60)
                client_class = MasterClientHandler(client, address)
                client_class.daemon = True
                client_class.start()
                client_class.get_name()
                self.clients[client_class.name] = client_class
            except Exception as e:
                if not self.to_stop:
                    self.logger.error("Error connecting client: %s", e)
        self.logger.debug("Master Networking Server is down")

    def ping(self):
        for name, client in self.clients.items():
            client.send_ping()

    def stopMaster(self):
        self.logger.debug("Shutting down Master Networking Server")
        self.to_stop = True
        for name, client in self.clients.items():
            client.stop()
        self.server.shutdown(socket.SHUT_RDWR)
        self.server.close()


class MasterClientHandler(threading.Thread):
    name = ""
    client = None
    address = None
    to_stop = False
    buffer = {}

    def __init__(self, client, address):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('MasterClientHandler')
        self.client = client
        self.address = address

    def run(self):
        self.listenToClient()

    def get_name(self):
        response = self.send_order(GET_NAME)
        name = response[globals.configuration.CONFIG_ANALYTIC_NAME]
        self.logger.debug("Client's name is %s", name)
        self.name = name

    def listenToClient(self):
        size = 1024
        data = ""
        while not self.to_stop:
            try:
                data = self.client.recv(size)
                if data and data != "":
                    data = str(data, "utf-8")
                    self.logger.debug("Order received: " + data)
                    data_json = json.loads(data)
                    if "response" in data_json:
                        self.buffer[data_json["order"]] = data_json["response"]
                    else:
                        response = ""
                        order = data_json["order"]

                        if order == GET_DATABASE:
                            response = self.order_get_database()
                        elif order == EXIT:
                            self.order_exit()
                        elif order == GET_CONFIGURATION:
                            response = self.order_get_configuration(data_json["parameters"][PARAMETER_PLUGIN_NAME],
                                                                    data_json["parameters"][PARAMETER_PLUGIN_TYPE])
                        elif order == NEW_ALERT:
                            self.order_new_alert(data_json["parameters"])

                        if response != "":
                            full_response = {}
                            full_response["order"] = order
                            full_response["response"] = response
                            self.client.send(bytes(json.dumps(full_response), "utf-8"))

            except Exception as e:
                self.logger.error("Error %s: ", e)
                self.client.close()
                self.to_stop = True
        self.set_node_status("DOWN")

    def send_order(self, order):
        json_order = {}
        json_order["order"] = order
        string_order = json.dumps(json_order)
        self.client.send(bytes(string_order, "utf-8"))
        response = None
        wait = True
        timeout = datetime.datetime.now() + datetime.timedelta(seconds=TIMEOUT)
        while wait and response == None:
            response = self.buffer.pop(order, None)
            if datetime.datetime.now() >= timeout:
                self.logger.debug("Timeout, no data received")
                wait = False
        self.logger.debug("Received data from client: %s", response)
        return response

    def send_ping(self):
        order = PING
        response = self.send_order(order)
        if response == PONG:
            self.set_node_status("UP")
        else:
            self.set_node_status("DOWN")

    def set_node_status(self, status):
        document = {}
        document["_key"] = self.name
        document["status"] = status
        globals.db.CreateDocument(NODES_COLLECTION, document)

    def stop(self):
        self.set_node_status("DOWN")
        self.to_stop = True

    def order_get_configuration(self, plugin_name, plugin_type):
        response = globals.configuration.load_configurationober(plugin_type, plugin_name)
        return response

    def order_get_database(self):
        data = {}
        data[globals.configuration.CONFIG_MASTER_DATABASE_HOST] = globals.configuration.db_host
        data[globals.configuration.CONFIG_MASTER_DATABASE_PORT] = globals.configuration.db_port
        data[globals.configuration.CONFIG_MASTER_DATABASE_USER] = globals.configuration.db_user
        data[globals.configuration.CONFIG_MASTER_DATABASE_PWD] = globals.configuration.db_pass
        return data

    def order_new_alert(self, parameters):
        engine = parameters[PARAMETER_PLUGIN_NAME]
        pars = parameters[PARAMETER_ALERT]
        globals.engines[engine].new_alert(pars)

    def order_exit(self):
        self.stop()


class Analytic(threading.Thread):
    host = "localhost"
    port = 5001
    client = None
    to_stop = False
    buffer = {}

    #: Initialize the analytic client.
    #: Parameters:
    #: host. Default: localhost
    #: port. Default: 5001
    def __init__(self, host="localhost", port=5001):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('Analytic')
        self.host = host
        self.port = int(port)

    def run(self):
        self.startClient()

    def stop(self):
        self.stopClient()

    def startClient(self):
        self.logger.debug("Starting Analytic Networking Server")
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect((self.host, self.port))
        while not self.to_stop:
            try:
                response = self.client.recv(1024)
                if response and response != "":
                    response = str(response, "utf-8")
                    self.logger.debug("Data received %s", response)
                    response_json = json.loads(response)
                    if "response" in response_json:
                        self.buffer[response_json["order"]] = response_json["response"]
                    else:
                        order = response_json["order"]
                        response_order = ""
                        if order == PING:
                            response_order = self.order_ping()
                        elif order == GET_NAME:
                            response_order = self.order_name()

                        if order != "":
                            full_response = {}
                            full_response["order"] = order
                            full_response["response"] = response_order
                            self.client.sendall(bytes(json.dumps(full_response), "utf-8"))
            except Exception as e:
                if not self.to_stop:
                    self.logger.error("Error receiving data from server: %s", e)
                else:
                    self.logger.info("Exiting from client socket")
        self.logger.debug("Analytic Networking Server is down")

    def send_order(self, order, parameters=None, wait=True):
        json_order = {}
        json_order["order"] = order
        if parameters != None:
            # parameters_json={}
            # for key, value in parameters.items():
            #    parameters_json[key]=value
            json_order["parameters"] = parameters
        string_order = json.dumps(json_order)
        self.client.sendall(bytes(string_order, "utf-8"))
        response = None
        timeout = datetime.datetime.now() + datetime.timedelta(seconds=TIMEOUT)
        while wait and response == None:
            response = self.buffer.pop(order, None)
            if datetime.datetime.now() >= timeout:
                self.logger.debug("Timeout, no data received")
                wait = False
        self.logger.debug("Received data from server: %s", response)
        return response

    def order_ping(self):
        return PONG

    def order_name(self):
        response = {}
        name = globals.configuration.name
        response[globals.configuration.CONFIG_ANALYTIC_NAME] = name
        return response

    def order_get_configuration(self, plugin_type, plugin_name):
        parameters = {}
        parameters[PARAMETER_PLUGIN_TYPE] = plugin_type
        parameters[PARAMETER_PLUGIN_NAME] = plugin_name
        response = self.send_order(GET_CONFIGURATION, parameters)
        return response

    def order_new_alert(self, engine, pars):
        parameters = {}
        parameters[PARAMETER_PLUGIN_NAME] = engine
        parameters[PARAMETER_ALERT] = pars
        response = self.send_order(NEW_ALERT, parameters, False)
        return response

    def stopClient(self):
        self.logger.debug("Shutting down Analytic Networking Server")
        self.send_order(EXIT, False)
        self.to_stop = True
        self.client.shutdown(socket.SHUT_RDWR)
        self.client.close()
