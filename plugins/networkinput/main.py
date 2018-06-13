import datetime
import json
import logging
import socket
import io
import threading
from splitstream import splitfile
from plugins import InputPlugin as ip

class NetworkInput(ip.InputPlugin):
    
    CONFIG_PORT="port"
    CONFIG_HOST="host"
    
    port=5000
    host="localhost"
    server=None
    factory=None
    callback=None
    to_stop=False
    clients={}

    def __init__(self):
        super(NetworkInput, self).__init__()
        self.logger = logging.getLogger('NetworkInput')
        self.name="NetworkInput" 
        self.description="Load data from json received by a TCP port"
        self.port=5000

    def initialize_analytic(self):
        super(NetworkInput, self).initialize_analytic()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, int(self.port)))
        self.logger.debug("Running server")
        self.server.listen(5)
    
    def create_configuration(self):
        configuration={}
        configuration[self.CONFIG_ENABLE]=self.enable
        configuration[self.CONFIG_PORT]=self.port
        configuration[self.CONFIG_HOST]=self.host
        return configuration

    def load_data(self):
        try:
            client, address = self.server.accept()
            client.settimeout(60)
            client_class=NetworkInputServerHandler(client, address, self.callback)
            client_class.daemon = True
            client_class.start()
            client_class.get_name()
            self.clients[client_class.name]=client_class
        except Exception as e:
            if self.to_stop==True:
                self.logger.error("Error connecting client: %s", e)

    def stop(self):
        super(NetworkInput, self).stop()
        self.logger.debug("Plugin: stop")
        for name, client in self.clients.items():
            client.stop()
        if self.server!=None:
            self.server.shutdown(socket.SHUT_RDWR)
            self.server.close()


class NetworkInputServerHandler (threading.Thread):
    callback = None
    buffer = None
    last_time = None
    client = None
    address = None
    to_stop = False
    blocked = False

    def __init__(self, client, address, callback):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger('NetworkInputServerHandler')
        self.client=client
        self.address=address
        self.callback=callback
        self.last_time=datetime.datetime.now()
        self.buffer=""

    def run(self):
        self.listenToClient()

    def listenToClient(self):
        size = 1024
        data = ""
        while self.to_stop == False:
            try:
                data = self.client.recv(size)
                if data and data != "":
                    data = str(data, "utf-8")
                    self.dataReceived(data)
            except Exception as e:
                self.logger.error("Error %s: ", e)
                self.client.close()
                self.to_stop=True


    def stop(self):
        self.to_stop=True

    def dataReceived(self, data):
        if (datetime.datetime.now() - self.last_time).total_seconds() >= 5:
            self.logger.debug("Resetting buffer")
            self.buffer = ""
        self.logger.debug("Data received %s, with buffer %s", data, self.buffer)
        self.last_time = datetime.datetime.now()

        while self.blocked:
            pass

        self.blocked = True

        if self.buffer == None:
            self.buffer = data
        else:
            self.buffer += data

        data2 = self.buffer.encode("utf-8")
        jdata = self.generate_jsons(data2)

        self.blocked = False

        if self.callback != None and data != None:
            if jdata != None and len(jdata) > 0:
                try:
                    for js in jdata:
                        self.logger.debug("Detected json data %s", js)
                        self.callback(json.loads(js))
                except Exception as e:
                    self.logger.error("Error calling callback function: %s", e)

        '''
        self.buffer += data#.decode("utf-8")
        data = self.buffer.encode("utf-8")

        if self.callback != None and data != None:
            jdata = self.generate_jsons(data)
            
            if jdata != None and len(jdata) > 0:
                try:
                    for js in jdata:
                        self.logger.debug("Detected json data %s", js)
                        self.callback(json.loads(js.decode("utf-8")))
                except Exception as e:
                    self.logger.error("Error calling callback function: %s", e)
        '''
        if len(self.buffer) > (1024*1024*1024):
            self.buffer = ""

    def generate_jsons(self, data):
        result = []
        f = io.BytesIO(data)
        buffer = data
        for js in splitfile(f, format="json"):
            #buffer = data[len(js):]
            #self.buffer = buffer.decode("utf-8")
            #self.buffer = buffer.decode("utf-8")
            #result.append(js)
            temp = js.decode("utf-8")
            self.buffer = self.buffer[len(temp):]
            result.append(temp)
        return result
