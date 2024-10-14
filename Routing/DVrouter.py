import sys
from collections import defaultdict
from router import Router
from packet import Packet
from json import dumps, loads
from networkx import Graph,bellman_ford_path


class DVrouter(Router):
    """Distance vector routing protocol implementation."""

    def __init__(self, addr, heartbeatTime):
        """TODO: add your own class fields and initialization code here"""
        Router.__init__(self, addr)  # initialize superclass - don't remove
        self.heartbeatTime = heartbeatTime
        self.last_time = 0
        # Hints: initialize local state
        
        self.distance_vector = dict()
        self.distance_vector[self.addr] = dict()
        
        self.neighbour_ports = dict()
        self.port_neighbours = dict()

        self.routing_table = dict()
        

    def handlePacket(self, port, packet):
        """TODO: process incoming packet"""
        if packet.isTraceroute():
            # Sending packet based on forwarding table
            possible_forwards = self.routing_table.keys()
            
            if packet.dstAddr in possible_forwards:
                try:
                    # checking port dictionary for port leading to correct neighbour in routing table
                    neigh_port = self.neighbour_ports[self.routing_table[packet.dstAddr]]
                    self.send(neigh_port, packet)
                except:
                    pass

            else:
                # The packet should be dropped
                return

        else:
            # Extracting information from packet
            x = loads(packet.getContent())

            # If received distance vector is different from current distance vector
            if self.compare_vectors(x):

                # Updating local copy of distance vector
                self.distance_vector = x

                # Updating forwarding table
                self.update_table()

                # Forwarding distance vector to neighbours
                self.start_flood()
                

    def compare_vectors (self,recv_dv):
        # Function to compare local distance vector and received distance vector
        for key,value in recv_dv.items():
            try:
                if self.distance_vector[key] != value:
                    return True
            except:
                pass
            
        return False

    def update_table(self):
        # Function to update the forwarding table

        # Creating networkx Graph 
        temp_graph = Graph(undirected=True)
        
        for key, value in self.distance_vector.items():
            for x,y in value.items():
                temp_graph.add_edge(key,x,weight=y)

        # Calculating paths using Bellman Ford
        for key, value in self.distance_vector.items():
            for x,y in value.items():
                try:
                    path = bellman_ford_path(temp_graph,self.addr,x)
                    # Updating routing table
                    self.routing_table[x] = path[1]
                    (self.distance_vector[self.addr])[x] = path.total_cost
                except:
                    pass


    def get_neighbours(self):
        # Function to get neighbours of current router
        neigh_list = []
        for key, value in self.distance_vector.items():

            if key == self.addr:
                neigh_list += value.keys()

        return neigh_list

    def start_flood(self):
        # Function to broadcast distance vector to all neighbours

        # Converting distance vector into json string
        pack_cont = dumps(self.distance_vector)
        neighbours = self.get_neighbours()

        for neigh in neighbours:
            try:
                flood_pack = Packet(Packet.ROUTING,self.addr,neigh,pack_cont)
                self.send(self.neighbour_ports[neigh],flood_pack)
            except:
                pass



    def handleNewLink(self, port, endpoint, cost):
        """TODO: handle new link"""
        
        # Updating dictionaries
        self.neighbour_ports[endpoint] = port
        self.port_neighbours[port] = endpoint

        # Updating distance vector
        self.distance_vector[self.addr][endpoint] = cost
        self.distance_vector[endpoint] = dict()

        # Updating the forwarding table
        self.update_table()

        # Broadcasting the distance vector of this router to neighbors
        self.start_flood()



    def handleRemoveLink(self, port):
        """TODO: handle removed link"""
        lost_neighbour = self.port_neighbours[port]

        # Updating dictionaries if such an edge existed
        try:
            del (self.distance_vector[self.addr])[lost_neighbour]
            del self.neighbour_ports[lost_neighbour]
            del self.port_neighbours[port]
        except:
            pass

        # Updating the forwarding table
        self.update_table()

        # Broadcasting the distance vector of this router to neighbors
        self.start_flood()


    def handleTime(self, timeMillisecs):
        """TODO: handle current time"""
        if timeMillisecs - self.last_time >= self.heartbeatTime:
            self.last_time = timeMillisecs
            # broadcast the distance vector of this router to neighbors
            self.start_flood()


    def debugString(self):
        """TODO: generate a string for debugging in network visualizer"""
        return ""
