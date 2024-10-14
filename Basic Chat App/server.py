'''
This module defines the behaviour of server in your Chat Application
'''
import sys
import getopt
import socket
import util
import threading 


class Server:
    '''
    This is the main Server Class. You will to write Server code inside this class.
    '''

    def __init__(self, dest, port):
        self.server_addr = dest
        self.server_port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(None)
        self.sock.bind((self.server_addr, self.server_port))
        self.userlist = []
        self.userdict = {}
        self.reverse_dict = {}


    def start(self):
        '''
        Main loop.
        continue receiving messages from Clients and processing it
        '''
        self.sock.listen()

        while True:
            try:
                conn, addr = self.sock.accept()
                t = threading.Thread(target = self.handle_clients, args = (conn,addr))
                t.start()
            except Exception as e:
                # print("Inside except: ")
                print(e)
                self.sock.close()

    def handle_clients (self, conn, addr):
        while True:
            try:
                msg = conn.recv(8000)
                msg = msg.decode('utf-8')
                temp_msg = msg.split()

                if len(temp_msg) > 0:
                    if temp_msg[0] == "join":
                        if self.join(temp_msg[1], conn, addr):
                            break
                    elif  temp_msg[0] == "disconnect":
                        self.disconnect(temp_msg[1],  conn, addr)
                        break
                    elif temp_msg[0] == "request_users_list":
                        self.user_list_request(msg,  conn, addr)
                    elif temp_msg[0] == "send_message": 
                        self.message_sending(msg,  conn, addr)
                    elif temp_msg[0] == "send_file":
                        self.file_sending(msg,  conn, addr)
                    else:
                        self.unknown_msg(msg,  conn, addr)
            
            except Exception as e:
                # print("Inside except: ")
                print(e) 
                


    def join (self, msg_username, conn,  msg_start_addr):
        failure_bool = True
        
        if len(self.userlist) >= util.MAX_NUM_CLIENTS:
            err_msg = util.make_message ("err_server_full",2)
            conn.send(err_msg.encode('utf-8'))
            conn.close()
            print("disconnected: server full")

        elif msg_username in self.userlist:
            err_msg = util.make_message ("err_username_unavailable",2)
            conn.send(err_msg.encode('utf-8'))
            conn.close()
            print("disconnected: username not available")
        
        else:
            (self.userlist).append(msg_username)
            self.userdict[msg_start_addr] = msg_username
            self.reverse_dict[msg_username] = conn
            print("join:",msg_username)
            failure_bool = False
        
        return failure_bool

        
    def disconnect (self, msg_username, conn, msg_start_addr):
        if msg_username in self.userlist:
            (self.userlist).remove(msg_username)

        conn.close()
        print("disconnected: "+msg_username)


    def user_list_request (self, msg, conn, msg_start_addr):
        # Is there a need to sort the list?
        response_msg = util.make_message ("response_users_list",3, util.server_list_to_string(self.userlist))
        conn.send(response_msg.encode('utf-8'))
        print("request_users_list:", self.userdict[msg_start_addr])


    def message_sending (self, msg, conn, msg_start_addr):
        sender_name = self.userdict[msg_start_addr]
        print("msg:",sender_name)

        temp_msg = msg.split()
        num_forward = temp_msg[1]
        forward_client_list = []

        counter = 2
        for k in range(int(num_forward)):
            if temp_msg[2+k] in self.userlist:
                rec_addr = self.reverse_dict[temp_msg[2+k]]
                forward_client_list.append(rec_addr)
                final_name = temp_msg[2+k]
            else:
                print("msg: "+ sender_name + " to non-existent user "+ temp_msg[2+k])
            
            counter += 1

        forwarded_msg = util.server_list_to_string(temp_msg[counter:])
        formatted_forward = util.make_message("forward_message",4,sender_name+":"+forwarded_msg)

        # Remove duplicates or send twice?
        forward_client_list = set(forward_client_list)
        forward_client_list = list(forward_client_list)

        for conns in forward_client_list:
            conns.send(formatted_forward.encode('utf-8'))



    def file_sending (self, msg, conn, msg_start_addr):
        sender_name = self.userdict[msg_start_addr]

        print("file:",sender_name)
        msg = msg.replace("send_file ","")
        
        temp_msg = msg.split()
        num_forward = temp_msg[1]
        forward_client_list = []

        counter = 2
        for k in range(int(num_forward)):
            if temp_msg[2+k] in self.userlist:
                rec_addr = self.reverse_dict[temp_msg[2+k]]
                forward_client_list.append(rec_addr)
                final_name = temp_msg[2+k]
            else:
                print("file: "+ sender_name + " to non-existent user "+ temp_msg[2+k])
            counter += 1

        forwarded_file = util.server_list_to_string(temp_msg[counter:])
        formatted_forward = util.make_message("forward_file",4,sender_name+": "+forwarded_file)

        for conns in forward_client_list:
            conns.send(formatted_forward.encode('utf-8'))



    
    def unknown_msg (self,msg, conn, msg_start_addr):
        print("Received unknown message: ")
        print(msg,msg_start_addr)

        unknown_str = "disconnected: " + self.userdict[msg_start_addr] + "sent unknown command"

        err_msg = util.make_message ("err_unknown_message",2)
        conn.send(err_msg.encode('utf-8'))
        conn.close()
        print(unknown_str)


# Do not change this part of code


if __name__ == "__main__":
    def helper():
        '''
        This function is just for the sake of our module completion
        '''
        print("Server")
        print("-p PORT | --port=PORT The server port, defaults to 15000")
        print("-a ADDRESS | --address=ADDRESS The server ip or hostname, defaults to localhost")
        print("-h | --help Print this help")

    try:
        OPTS, ARGS = getopt.getopt(sys.argv[1:],
                                   "p:a", ["port=", "address="])
    except getopt.GetoptError:
        helper()
        exit()

    PORT = 15000
    DEST = "localhost"

    for o, a in OPTS:
        if o in ("-p", "--port="):
            PORT = int(a)
        elif o in ("-a", "--address="):
            DEST = a

    SERVER = Server(DEST, PORT)
    try:
        SERVER.start()
    except (KeyboardInterrupt, SystemExit):
        exit()
