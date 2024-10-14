#!/usr/bin/python
import os
import socket
import subprocess
import time
import random
import signal
import util
from Tests import SingleClientTest, BasicTest, MultipleClientsTest, ErrorHandlingTest, FileSharingTest


def tests_to_run(forwarder):
    SingleClientTest.SingleClientTest(forwarder, "SingleClient")
    MultipleClientsTest.MultipleClientsTest(forwarder, "MultipleClients")
    FileSharingTest.FileSharingTest(forwarder, 'FileSharing')
    ErrorHandlingTest.ErrorHandlingTest(forwarder, "ErrorHandling")


class Forwarder(object):
    def __init__(self, sender_path, receiver_path, port):
        if not os.path.exists(sender_path):
            raise ValueError("Could not find sender path: %s" % sender_path)
        self.sender_path = sender_path

        if not os.path.exists(receiver_path):
            raise ValueError("Could not find receiver path: %s" %
                             receiver_path)
        self.receiver_path = receiver_path

        self.tests = {}  # test object => testName
        self.current_test = None
        self.out_queue = []
        self.in_queue = []
        self.tick_interval = 0.001  # 1ms
        self.last_tick = time.time()
        self.timeout = 6.  # seconds

        # network stuff
        self.port = port
        self.middle_clientside = {}  # Man in the middle sockets that connects with clients
        self.middle_serverside = {}  # Man in the middle sockets that connects with server
        self.senders = {}
        self.receiver_port = self.port + 1
        self.receiver_addr = None

    def _tick(self):
        self.current_test.handle_tick(self.tick_interval)
        for p, user in self.out_queue:
            self._send(p, user)
        self.out_queue = []

    def _send(self, message, user):
        if message.receiver == "clientside":
            self.middle_clientside[user].send(message.message)
        elif message.receiver == "serverside":
            self.middle_serverside[user].send(message.message)

    def register_test(self, testcase, testName):
        assert isinstance(testcase, BasicTest.BasicTest)
        self.tests[testcase] = testName

    def execute_tests(self):
        for t in self.tests:
            self.port = random.randint(2000, 65500)
            self.current_test = t
            self.current_test.set_state()

            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind(('', self.port))
            self.sock.listen()
            self.middle_clientside = {}  # Man in the middle sockets that connects with clients
            self.middle_serverside = {}  # Man in the middle sockets that connects with server
            for client in sorted(self.current_test.client_stdin.keys()):
                self.middle_serverside[client] = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                self.middle_serverside[client].settimeout(
                    0.01)  # make this a very short timeout

            print(("Testing %s" % self.tests[t]))
            self.start()

    def handle_receive(self, message, sender, user):
        if sender == "clientside":
            m = MessageWrapper(message, "serverside")
        elif sender == "serverside":
            m = MessageWrapper(message, "clientside")

        self.in_queue.append((m, user))
        self.current_test.handle_message()

    def start(self):
        self.receiver_addr = ('127.0.0.1', self.receiver_port)
        self.recv_outfile = "server_out"

        recv_out = open(self.recv_outfile, "w")
        receiver = subprocess.Popen(
            ["python3", self.receiver_path, "-p",
             str(self.receiver_port)],
            stdout=recv_out)
        time.sleep(0.2)  # make sure the receiver is started first
        self.senders = {}
        sender_out = {}
        for i in sorted(list(self.current_test.client_stdin.keys())):
            u = i
            sender_out[i] = open("client_" + i, "w")
            if "duplicate" in i:
                u = i[:7]
            self.senders[i] = subprocess.Popen([
                "python3", self.sender_path, "-p",
                str(self.port), "-u", u
            ],
                stdin=subprocess.PIPE,
                stdout=sender_out[i])

            conn, addr = self.sock.accept()
            conn.settimeout(0.01)
            self.middle_clientside[i] = conn
            self.middle_serverside[i].connect(self.receiver_addr)

        try:
            client_stdin = dict(self.current_test.client_stdin)
            start_time = time.time()
            self.last_tick = time.time()
            while None in [self.senders[s].poll() for s in self.senders]:
                for i in sorted(list(self.current_test.client_stdin.keys())):
                    try:
                        message = self.middle_clientside[i].recv(4096)
                        if len(message) != 0:
                            self.handle_receive(message, "clientside", i)
                    except socket.timeout:
                        pass
                    try:
                        message = self.middle_serverside[i].recv(4096)
                        if len(message) != 0:
                            self.handle_receive(message, "serverside", i)
                    except socket.timeout:
                        pass
                    if time.time() - self.last_tick > self.tick_interval:
                        self.last_tick = time.time()
                        self._tick()
                    if time.time() - start_time > self.timeout:
                        raise Exception("Test timed out!")
            # in case message is not received but client have terminated
            while bool(client_stdin):
                for i in list(client_stdin.keys()):
                    try:
                        message = self.middle_clientside[i].recv(4096)
                        if len(message) != 0:
                            self.handle_receive(message, "clientside", i)
                        else:
                            del client_stdin[i]
                    except socket.timeout:
                        pass
            self._tick()
        except (KeyboardInterrupt, SystemExit):
            exit()
        finally:
            for sender in self.senders:
                if self.senders[sender].poll() is None:
                    self.senders[sender].send_signal(signal.SIGINT)
                sender_out[sender].close()
            receiver.send_signal(signal.SIGINT)
            recv_out.flush()
            recv_out.close()

        if not os.path.exists(self.recv_outfile):
            raise RuntimeError("No data received by receiver!")
        time.sleep(1)
        try:
            self.current_test.result()
        except Exception as e:
            print("Test Failed!", e)


class MessageWrapper(object):
    def __init__(self, message, receiver):
        self.message = message
        self.receiver = receiver


if __name__ == "__main__":
    import getopt
    import sys

    def usage():
        print("Tests for Chat Application")
        print(
            "-c CLIENT | --client CLIENT The path to Client implementation (default: client.py)"
        )
        print(
            "-s SERVER | --server SERVER The path to the Server implementation (default: server.py)"
        )
        print("-h | --help Print this usage message")

    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:s:",
                                   ["client=", "server="])
    except:
        usage()
        exit()

    port = random.randint(2000, 65500)
    sender = "client.py"
    receiver = "server.py"

    for o, a in opts:
        if o in ("-c", "--client"):
            sender = a
        elif o in ("-s", "--server"):
            receiver = a

    f = Forwarder(sender, receiver, port)
    tests_to_run(f)
    f.execute_tests()
