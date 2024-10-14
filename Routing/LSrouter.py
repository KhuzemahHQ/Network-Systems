import sys
from collections import defaultdict
from router import Router
from packet import Packet
from json import dumps, loads
from dijkstar import Graph,find_path

# python visualize_network.py test1.json LS

class LSrouter(Router):
    """Link state routing protocol implementation."""

    def __init__(self, addr, heartbeatTime):
        """TODO: add your own class fields and initialization code here"""
        Router.__init__(self, addr)  # initialize superclass - don't remove
        self.heartbeatTime = heartbeatTime
        self.last_time = 0

        self.link_state = []
        self.neighbour_ports = dict()
        # Reverse of above dictionary
        self.port_neighbours = dict()
        self.routing_table = dict()
        self.global_view = Graph(undirected = True)

        self.curr_seq = 0

        # For debugging:
        self.no_path = set()
        self.dead_end = []

    def handlePacket(self, port, packet):
        """TODO: process incoming packet"""
        if packet.isTraceroute():
            
            # Finding list of neighbouring ports
            possible_forwards = self.routing_table.keys()
            
            if packet.dstAddr in possible_forwards:
                try:
                    # checking port dictionary for port leading to correct neighbour in routing table
                    neigh_port = self.neighbour_ports[self.routing_table[packet.dstAddr]]
                    self.send(neigh_port, packet)
                except:
                    pass

            else:
                # Dead-end added to dead_end list for debugging purposes
                self.dead_end.append((self.addr,packet.dstAddr))
                # The packet should be dropped
                return
        else:

            # Extracting information from packet contents
            x = loads(packet.getContent())
            pack_seq_no = x["seq"]
            pack_link_state = x["ls"]

            # if the sequence number is higher and the received link state is different
            if pack_seq_no > self.curr_seq and self.compare_link_state(pack_link_state):
                # Updating local link state copy
                self.curr_seq = pack_seq_no
                self.link_state += pack_link_state
                self.clean_link_state()

                # Updating global view
                self.global_view = Graph(undirected = True)
                for edge in self.link_state:
                    self.global_view.add_edge(edge[0],edge[1],edge[2])
                
                # Updating routing table
                self.recompute_routing_table()

                # Forwarding this information to all neighbours except source of packet
                self.start_flood(packet.srcAddr)

            else:
                # Discarding packet if sequence number is old or link state is the same
                return

    def compare_link_state(self,x):
        # Function to compare received link state with local link state
        for element in x:
            if element not in self.link_state:
                return True
            
        return False
    
    def clean_link_state(self):
        # Function to remove duplicates that occur due to concatenating lists
        temp_list = []

        for element in self.link_state:
            if element not in temp_list:
                temp_list.append(element)

        self.link_state = temp_list


    def handleNewLink(self, port, endpoint, cost):
        """TODO: handle new link"""

        # Updating port and neighbour dictionaries
        self.neighbour_ports[endpoint] = port
        self.port_neighbours[port] = endpoint

        # Adding edge to global view
        self.global_view.add_edge(self.addr,endpoint,cost)

        # adding edge to local link state
        temp_edge = []
        temp_edge.append(self.addr)
        temp_edge.append(endpoint)
        temp_edge.append(cost)
        self.link_state.append(temp_edge)  

        # Updating routing table
        self.recompute_routing_table()

        # Broadcasting/Flooding information to all neighbours
        self.start_flood(self.addr)


    def handleRemoveLink(self, port):
        """TODO: handle removed link"""

        # Finding address of lost neighbour using port
        lost_neighbour = self.port_neighbours[port]

        # Removing edge from global view
        self.global_view.remove_edge(self.addr,lost_neighbour)

        # Removing edge from link state
        for edge in self.link_state:
            if edge[0] == lost_neighbour and edge[1] == self.addr:
                self.link_state.remove(edge)
            if edge[0] == self.addr and edge[1] == lost_neighbour:
                self.link_state.remove(edge)

        # Removing entries in neighbour port dictionaries
        del self.neighbour_ports[lost_neighbour]
        del self.port_neighbours[port]

        # Updating routing table
        self.recompute_routing_table()

        # Broadcasting/Flooding information to all neighbours
        self.start_flood(self.addr)


    def handleTime(self, timeMillisecs):
        """TODO: handle current time"""
        if timeMillisecs - self.last_time >= self.heartbeatTime:
            self.last_time = timeMillisecs

            # broadcasting the link state of this router to all neighbors
            self.start_flood(self.addr)

    def recompute_routing_table(self):
        # Function to recompute the routing table

        self.routing_table = dict()

        # for every edge in the local link state other than outgoing edges of current router (to exclude paths to oneself)
        for edge in self.link_state:
            if edge[0] != self.addr:
                try:
                    # Finding a path from this router to other first node of the edge 
                    path = find_path(self.global_view,self.addr,edge[0])
                    self.routing_table[edge[0]] = path.nodes[1]
                except:
                    # Adding to no-paths list for debugging purposes
                    self.no_path.add((self.addr,edge[0]))

            if edge[1] != self.addr:
                try:
                    # Finding a path from this router to other second node of the edge 
                    path = find_path(self.global_view,self.addr,edge[1])
                    self.routing_table[edge[1]] = path.nodes[1]
                except:
                    # Adding to no-paths list for debugging purposes
                    self.no_path.add((self.addr,edge[1]))

    def get_neighbours(self):
        # Function to return all nodes connected to this router
        neigh_list = []

        for edge in self.link_state:
            if edge[0] == self.addr:
                neigh_list.append(edge[1])
            elif edge[1] == self.addr:
                neigh_list.append(edge[0])

        return neigh_list

    def start_flood(self,src_addr):

        # Creating the packet content
        pack_cont = dict()
        pack_cont["ls"] = self.link_state
        pack_cont["seq"] = self.curr_seq + 1
        flood_pack_content = dumps(pack_cont)

        # Getting neighbours
        neighbours = self.get_neighbours()

        # Sending packet to all neighbours other than source address
        # If this function was called by handleRemoveLink or handleNewLink, the src_addr is current node which can't be a neighbour
        # so packet will be sent to all neighbours
        for neigh in neighbours:
            if neigh != src_addr:
                try:
                    flood_pack = Packet(Packet.ROUTING,self.addr,neigh,flood_pack_content)
                    self.send(self.neighbour_ports[neigh],flood_pack)
                except:
                    pass

    def debugString(self):
        """TODO: generate a string for debugging in network visualizer"""

        sq = str(self.curr_seq)
        ls = str(self.link_state)
        gv = str(self.global_view)
        rt = str(self.routing_table)
        neigh = str(self.neighbour_ports)
        rneigh = str(self.port_neighbours)
        np = str(self.no_path)
        de = str(self.dead_end)

        debug_str = sq + "\n link state: " + ls + "\n Global View: " + gv + "\n Routing Table: " + rt + "\n Neighs: " + neigh + "\n Reverse neighs: " + rneigh + "\n No paths: " + np + "\n Dead ends: " + de 

        return  debug_str
