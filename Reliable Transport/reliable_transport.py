from queue import Queue
from typing import Tuple
from socket import socket
import random
import util
import time 

Address = Tuple[str, int]

class MessageSender:
    '''
    DO NOT EDIT ANYTHING IN THIS CLASS
    '''

    def __init__(self, sock: socket, receiver_addr: Address, msg_id: int):
        self.__sock: socket = sock
        self.__receiver_addr = receiver_addr
        self.__msg_id = msg_id

    def send(self, packet: str):
        self.__sock.sendto(
            (f"s:{str(self.__msg_id)}:{packet}").encode("utf-8"),
            self.__receiver_addr)


class ReliableMessageSender(MessageSender):
    '''
    This class reliably delivers a message to a receiver.
    You have to implement the send_message and on_packet_received methods.
    You can use self.send(packet) to send a packet to the receiver.
    You can add as many helper functions as you want.
    '''

    def __init__(self, sock: socket, receiver_addr: Address, msg_id: int,
                 window_size: int):
        MessageSender.__init__(self, sock, receiver_addr, msg_id)
        '''
        This is the constructor of the class where you can define any class attributes.
        window_size is the size of your message transport window (the number of in-flight packets during message transmission).
        Ignore other arguments; they are passed to the parent class.
        You should immediately return from this function and not block.
        '''
        self.started_bool = False
        self.qu = Queue()
        self.window_size = window_size
        

    def on_packet_received(self, packet: str):
        '''
        TO BE IMPLEMENTED BY STUDENTS

        This method is invoked whenever a packet is received from the receiver.
        Ideally, only ACK packets should be received here.
        You would have to use a way to communicate these packets to the send_message method.
        One way is to use a queue: you can enqueue packets to it in this method, and dequeue them in send_message.
        You can also use the timeout argument of a queue's dequeue method to implement timeouts in this assignment.
        You should immediately return from this method and not block.
        '''

        if util.validate_checksum(packet):
            p_type, p_seq_no, p_data, p_checksum = util.parse_packet(packet)
            if p_type == "ack":
                # print("putting ack into queue: ", p_seq_no)
                self.qu.put(int(p_seq_no))
            else:
                print("Should not be getting any other type")


    def send_message(self, message: str):
        ''''
        TO BE IMPLEMENTED BY STUDENTS

        This method reliably sends the passed message to the receiver. 
        This method does not need to spawn a new thread and return immediately; it can block indefinitely until the message is completely received by the receiver. 
        You can send a packet to the receiver by calling self.send(...).

        Sender's logic:
        1) Break down the message into util.CHUNK_SIZE sized chunks.
        2) Choose a random sequence number to start the communication from.
        3) Reliably send a start packet. (i.e. wait for its ACK and resend the packet if the ACK is not received within util.TIME_OUT seconds.)
        4) Send out a window of data packets and wait for ACKs to slide the window appropriately.
        5) How to slide the window? Suppose that the current window starts at sequence number j. If you receive an ACK of sequence number k, such that k > j, send the subsequent k - j number of chunks. Note that the window now starts from sequence number j + (k - j).
        6) If you receive no ACKs for util.TIME_OUT seconds, resend all the packets in the current window.
        7) Once all the chunks have been reliably sent, reliably send an end packet.
        '''

        # Make n packets, each of size util.CHUNK_SIZE. 1 byte = 1 char

        n = util.CHUNK_SIZE
        chunks = [message[i:i+n] for i in range(0, len(message), n)]

        initial = random.randint(1, 1000)
        pack_dict = dict()
        sent_dict = dict()

        i = 1
        for chunk in chunks:
            pack_dict[initial+i] = (util.make_packet("data",initial+i,chunk))
            sent_dict[initial+i] = False
            i += 1
            
        message_size = len(chunks)

        start_packet = util.make_packet("start",initial)
        # print("Before sending start")
        while self.qu.empty() :
            self.send(start_packet)
            time.sleep(util.TIME_OUT)

        self.qu = Queue()
        # print("Window size is ",self.window_size)
        # print("Before sending data:", pack_dict.keys())
        current_pack = initial+1
        while self.ongoing_check(sent_dict):
            # print("current packet: ",current_pack)
            self.send_window(pack_dict,current_pack)
            sent_dict,current_pack  = self.update_sent_dict(sent_dict,current_pack)


        # print("Before sending end: ",current_pack, initial + message_size+ 1)
        end_packet = util.make_packet("end",initial + message_size+ 1)
        self.qu = Queue()
        while self.qu.empty() :
            self.send(end_packet)
            time.sleep(util.TIME_OUT)

        # print("After sending end")


    def send_window(self,msg_dict,start_index):

        for i in range(0,self.window_size):
            if (start_index+i) in msg_dict.keys():
                self.send(msg_dict[start_index+i])

    def ongoing_check(self,sent_dict):
        # print("sent_dict :",sent_dict)
        for element in sent_dict.values():
            if element == False:
                return True

        return False

    def update_sent_dict(self,sent_dict,curr_pack):
        time.sleep(util.TIME_OUT)
        y = curr_pack

        if not self.qu.empty():
            x = max(list(self.qu.queue))
            # print("Found max cum ack = ", x)
            y = x
            for key in sent_dict.keys():
                if key < x:
                    sent_dict[key] = True


        return sent_dict, y



class MessageReceiver:
    '''
    DO NOT EDIT ANYTHING IN THIS CLASS
    '''

    def __init__(self, sock: socket, sender_addr: Address, msg_id: int,
                 completed_message_q: Queue):
        self.__sock: socket = sock
        self.__sender_addr = sender_addr
        self.__msg_id = msg_id
        self.__completed_message_q = completed_message_q

    def send(self, packet: str):
        self.__sock.sendto(
            (f"r:{str(self.__msg_id)}:{packet}").encode("utf-8"),
            self.__sender_addr)

    def on_message_completed(self, message: str):
        self.__completed_message_q.put(message)


class ReliableMessageReceiver(MessageReceiver):
    '''
    This class reliably receives a message from a sender. 
    You have to implement the on_packet_received method. 
    You can use self.send(packet) to send a packet back to the sender, and will have to call self.on_message_completed(message) when the complete message is received.
    You can add as many helper functions as you want.
    '''

    def __init__(self, sock: socket, sender_addr: Address, msg_id: int,
                 completed_message_q: Queue):
        MessageReceiver.__init__(self, sock, sender_addr, msg_id,
                                 completed_message_q)
        '''
        This is the constructor of the class where you can define any class attributes to maintain state.
        You should immediately return from this function and not block.
        '''
        self.data_dict = dict()
        self.seq_list = []
        self.start_pack_no = 0
        self.final_pack_no = 0
        # print("Message reciever intialized")

    def on_packet_received(self, packet: str):
        '''
        TO BE IMPLEMENTED BY STUDENTS

        This method is invoked whenever a packet is received from the sender.
        You have to inspect the packet and determine what to do.
        You should immediately return from this method and not block.
        You can either ignore the packet, or send a corresponding ACK packet back to the sender by calling self.send(packet).
        If you determine that the sender has completely sent the message, call self.on_message_completed(message) with the completed message as its argument.

        Receiver's logic:
        1) When you receive a packet, validate its checksum and ignore it if it is corrupted.
        2) Inspect the packet_type and sequence number.
        3) If the packet type is "start", prepare to store incoming chunks of data in some data structure and send an ACK back to the sender with the received packet's sequence number + 1.
        4) If the packet type is "data", store it in an appropriate data type (if it is not a duplicate packet you already have stored), and send a corresponding cumulative ACK. (ACK with the sequence number for which all previous packets have been received).
        5) If the packet type is "end", assemble all the stored chunks into a message, call self.on_message_received(message) with the completed message, and send an ACK with the received packet's sequence number + 1.
        '''

        if util.validate_checksum (packet):
            p_type, p_seq_no, p_data, p_checksum = util.parse_packet(packet)
            # print("receiver got valid checksum packet with details: ")
            # print(p_type, p_seq_no, p_data, p_checksum)
            p_seq_no = int(p_seq_no)

            if p_type == "start":
                
                self.start_pack_no = p_seq_no
                first_ack = util.make_packet("ack",p_seq_no+1)
                self.send(first_ack)

            elif p_type == "end":
                self.final_pack_no = int(p_seq_no)
                # print("Receiver got end packet, had already gotten: ", self.seq_list)
                last_ack = util.make_packet("ack",p_seq_no+1)
                self.send(last_ack)
                self.on_message_completed(self.get_message())

            elif p_type == "data":
                if p_seq_no not in self.seq_list:
                    self.seq_list.append(p_seq_no)
                    self.seq_list.sort()
                    # print("Receiver got data packet:", p_seq_no)
                    self.data_dict[p_seq_no] = (p_data)

                next_ack = util.make_packet("ack",self.get_cumm_ack())
                self.send(next_ack)

            else:
                print("Should not be getting any other type")

    def get_message(self):
        mess = ""
        for i in range(self.start_pack_no + 1, self.final_pack_no):
            if self.data_dict[i]:
                mess += self.data_dict[i]

        return mess

    def get_cumm_ack(self):

        # print("sequence list:", self.seq_list)
        x = self.start_pack_no
        for element in self.seq_list:
            if element == x + 1 or element == x:
                x = element
            else:
                break 

        # print("next ack from sequence ", self.seq_list,"should be: ", x+1)
        return x+1


