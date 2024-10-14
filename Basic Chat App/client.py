'''
This module defines the behaviour of a client in your Chat Application
'''
import sys
import getopt
import socket
import random
from threading import Thread
import os
import util


'''
Write your code inside this class. 
In the start() function, you will read user-input and act accordingly.
receive_handler() function is running another thread and you have to listen 
for incoming messages in this function.
'''


class Client:
    '''
    This is the main Client Class. 
    '''

    def __init__(self, username, dest, port):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(None)
        self.name = username
        self.conn_bool = False

    def start(self):
        '''
        Main Loop is here
        Start by sending the server a JOIN message.
        Waits for userinput and then process it
        '''
        try:
            self.sock.connect((self.server_addr, self.server_port))
            self.conn_bool = True
        except Exception as e:
                # print("Inside except: ")
                # print(e)
                sys.exit()

        join_message = util.make_message ("join",1,self.name)
        self.sock.send(join_message.encode('utf-8'))

        while True:
            try:
                msg = input()
                temp_msg = msg.split()
                if msg == "list":
                    msg = util.make_message ("request_users_list",2)
                    self.sock.send(msg.encode('utf-8'))
                    
                elif msg == "quit":
                    msg = util.make_message ("disconnect",1,self.name)
                    self.sock.send(msg.encode('utf-8'))
                    print("quitting")
                    self.sock.close()
                    break

                elif msg == "help":
                    print("list -- Get list of available users")
                    print("quit -- Exit the program")
                    print("msg <number_of_users> <username1> <username2> ... <message> -- Send message to users")
                    print("file <number_of_users> <username1> <username2> ... <file_name> -- Send file to users")
                    print("help -- Get list of possible commands")

                elif len(temp_msg) > 0:
                    if temp_msg[0] == "msg":
                        request = util.make_message("send_message",4, util.client_list_to_string(temp_msg[1:]))
                        self.sock.send(request.encode('utf-8'))

                    elif temp_msg[0] == "file":
                        request = self.share_file(msg)
                        self.sock.send(request.encode('utf-8'))

                    else:
                        print("incorrect userinput format")
                        # self.sock.send(msg.encode('utf-8'))

            except Exception as e:
                # print("Inside except: ")
                # print(e)
                sys.exit()
            


    def receive_handler(self):
        '''
        Waits for a message from server and process it accordingly
        '''

        while True:
            if self.conn_bool == True:
                break

        while True:
            try:
                msg = self.sock.recv(8000)
                msg = msg.decode('utf-8')
                # print(f"Message sent by server: {msg}")

                temp_msg = msg.split()

                if len(temp_msg) > 0:
                    if temp_msg[0] == "response_users_list":
                        print("list:",util.client_list_to_string(temp_msg[1:]))

                    elif temp_msg[0] == "err_server_full":
                        self.close_connection()
                        print("disconnected: server full")
                        break

                    elif temp_msg[0] == "err_username_unavailable":
                        self.close_connection()
                        print("disconnected: username not available")
                        break

                    elif temp_msg[0] == "err_unknown_message":
                        self.close_connection()
                        print("disconnected: server received an unknown command")
                        break

                    elif temp_msg[0] == "forward_message":
                        forwarded_message = msg.replace("forward_message ","")
                        print("msg:",forwarded_message)

                    elif temp_msg[0] == "forward_file":
                        self.file_receiver(msg)

            except Exception as e:
                # print("Inside except: ")
                # print(e)
                sys.exit()

    def close_connection(self):
        self.sock.close()

    def share_file (self, msg):
        try:
            first_part = msg

            temp_msg = msg.split()
            file_name = temp_msg[-1]

            f = open(file_name,"r")
            lines = f.read()

            second_part = first_part + " " + lines
            file_msg = util.make_message("send_file",4,second_part)

            return file_msg

        except Exception as e:
                # print("Inside except: ")
                # print(e)
                sys.exit()

    def file_receiver (self,msg):

        temp_msg = msg.split()
        print("file: "+ temp_msg[1] + " " + temp_msg[2])

        new_file_name = self.name + "_" + temp_msg[2]
        # new_file_name = temp_msg[2]

        # print(new_file_name)
        second_part = msg.split(temp_msg[2] + " ")
        second_part = second_part[1]

        f = open(new_file_name, "w")
        f.write(second_part)
        f.close()


# Do not change this part of code
if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our Client module completion
        '''
        print("Client")
        print("-u username | --user=username The username of Client")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-h | --help Print this help")
    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "u:p:a", ["user=", "port=", "address="])
    except getopt.error:
        helper()
        exit(1)

    PORT = 15000
    DEST = "localhost"
    USER_NAME = None
    for o, a in OPTS:
        if o in ("-u", "--user="):
            USER_NAME = a
        elif o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a

    if USER_NAME is None:
        print("Missing Username.")
        helper()
        exit(1)

    S = Client(USER_NAME, DEST, PORT)
    try:
        # Start receiving Messages
        T = Thread(target=S.receive_handler)
        T.daemon = True
        T.start()
        # Start Client
        S.start()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
