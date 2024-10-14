import socket 
import threading
import os
import time
import hashlib
from json import loads, dumps

# I kept the backup for the files in the successor, 
# so that the files do not even have to be rehashed, and will already be at the 
# successor when a node fails.
# The successor immediately notices that its previous predecessor failed 
# when it gets a ping from a different address.
# Now, this node can simply, re-label the back-up as its own files.
# This way, the file is never unavailable

class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host,self.port)
		self.predecessor = (self.host,self.port)

		# Opening a thread for pinging
		threading.Thread(target = self.periodic_ping).start()
		# Setting up a backup successor in case the current sucessor fails
		# so that node failure does not disconnect the hash ring
		self.backup_succ = (self.host,self.port)
		# Saving the directory of the current node
		self.dir = self.host+"_"+str(self.port)+"/"
		# Making a constant number of bits for transmission, following the sendFile and ReceiveFile formula
		self.transmission_limit = 1024 




	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

	def lookup_node(self,key):		
		# Function to check if a node is in the range of current node 
		hash_succ = self.hasher(self.successor[0]+str(self.successor[1]))

		if key >= self.key and key < hash_succ:
			# Generic case
			return True
		
		elif self.key > hash_succ:
			# Special case where there is a decrease in key value from current node to successor
			# This is because the hash ring loops around and returns to value of 0
			if key < hash_succ and key < self.key:
				# If the key is after the resetting of the hash ring
				return True
			if key > hash_succ and key > self.key:
				# If the key is before the resetting of the hash ring
				return True
			
		else:
			return False
		
	def lookup_file(self,file_name):
		# Function to check if a file is in the range of current node 
		file_key = self.hasher(file_name)
		hash_pred = self.hasher(self.predecessor[0] + str(self.predecessor[1]))
		
		if (file_key < self.key and file_key > hash_pred):
			# Generic case
			return True
		
		elif (self.key < hash_pred):
			# Special case where there is a decrease in key value from predecessor to successor
			# This is because the hash ring loops around and returns to value of 0
			if file_key > self.key and file_key > hash_pred:
				# If the key is before the resetting of the hash ring
				return True
			if file_key < self.key and file_key < hash_pred:
				# If the key is after the resetting of the hash ring
				return True
			
		else:
			return False

	def handleConnection(self, client, addr):
		
		# Reading the input and loading it
		msg = client.recv(self.transmission_limit)
		msg = msg.decode()
		if (msg == ''):
			print("empty message")
			return
		msg = loads(msg)

		msg_type = msg[0]
		msg_sender = tuple(msg[-1])

		# Having a different response, depending on what the messgae received was
		if msg_type == "join":
			sender_key = msg[1]
			if self.predecessor == (self.host,self.port) and self.successor == (self.host,self.port):
				# Edge case where there is only one node in the DHT
				self.successor = msg_sender
				self.predecessor = msg_sender

				response_msg = ["join_response", (self.host,self.port), (self.host,self.port)]
				response_msg = dumps(response_msg)
				response_msg = response_msg.encode('utf-8')
				sock = socket.socket()
				sock.connect(msg_sender)
				sock.send(response_msg)

			else:
				if self.lookup_node(sender_key):
					# Directly sending the address of current node and current node's successor to the new node
					response_msg = ["join_response", self.successor, (self.host,self.port)]
					response_msg = dumps(response_msg)
					response_msg = response_msg.encode('utf-8')

					sock = socket.socket()
					sock.connect(msg_sender)
					sock.send(response_msg)

					# Need to update successor's successor's predecessor. It is being indirectly done by pinging, 
					# but could also be done in the following manner:

					# response_msg2 = ["update_pred", msg_sender, (self.host,self.port)]
					# response_msg2 = dumps(response_msg2)
					# response_msg2 = response_msg2.encode('utf-8')
					# sock2 = socket.socket()
					# sock2.connect(self.successor)
					# sock2.send(response_msg2)

					self.successor = msg_sender

				else: 
					# Forwarding join request to successor if node does not belong here
					response_msg = ["join", sender_key, msg_sender]
					response_msg = dumps(response_msg)
					response_msg = response_msg.encode('utf-8')

					sock = socket.socket()
					sock.connect(self.successor)
					sock.send(response_msg)


		elif msg_type == "join_response":
			# The new node will receive this message and sets its predecessor and successor accordingly
			new_succ = tuple(msg[1])
			new_pred = tuple(msg[2])
			self.successor = new_succ
			self.predecessor = new_pred

		elif msg_type == "succ_left":
			# When a node leaves, it sends this message to its predecessor
			new_succ = tuple(msg[1])
			self.successor = new_succ

			response_msg = ["update_pred", (self.host,self.port), (self.host,self.port)]
			response_msg = dumps(response_msg)
			response_msg = response_msg.encode('utf-8')

			sock = socket.socket()
			sock.connect(self.successor)
			sock.send(response_msg)

		elif msg_type == "update_pred":
			# When a node leaves, its predecessor is informed, which then informs its old successor
			new_pred = tuple(msg[1])
			self.predecessor = new_pred

		elif msg_type == "pred_left":
			# When a node leaves, it sends this message to its successor so it can adjust its files and backups
			missing_files = msg[1]

			for file in missing_files:
				if file not in self.files:
					# self.receiveFile(client,file)
					self.files.append(file)

			missing_backup = msg[2]
			for file in missing_backup:
				if file not in self.backUpFiles:
					# self.receiveFile(client,file)
					self.backUpFiles.append(file)

		elif msg_type == "put":
			# When a node knows the file does not belong with it, it send this message to its successor
			fname = msg[1]
			self.put(fname)

			# self.receiveFile(client,fname)

		elif msg_type == "get":

			fname = msg[1]

			if msg_sender == (self.host,self.port):
				# We have reached a dead-end and none of the previous nodes had the file in their directory
				# Sending a not found message all the way back to the source
				response_msg = ["Not_found",(self.host, self.port),(self.host, self.port)]
				response_msg = dumps(response_msg)
				response_msg = response_msg.encode('utf-8')
				client.send(response_msg)

			else:
				if fname in self.files:
					response_msg = ["get_response",(self.host, self.port),(self.host, self.port)]
					response_msg = dumps(response_msg)
					response_msg = response_msg.encode('utf-8')
					client.send(response_msg)

					# new_sock = socket.socket()
					# new_sock.connect(msg_sender)
					# self.sendFile(new_sock,fname)

				else:
					# Forwarding to successor
					sock = socket.socket()
					sock.connect(self.successor)

					forward_msg = ["get", fname, msg_sender]
					forward_msg = dumps(forward_msg)
					forward_msg = forward_msg.encode('utf-8')
					sock.send(forward_msg)

					# Getting response from successor and backtracking all the way till first node that sent request
					returned_msg = sock.recv(self.transmission_limit)
					returned_msg = returned_msg.decode()
					returned_msg = returned_msg.encode('utf-8')
					client.send(returned_msg)
					
		elif msg_type == "ping":
			# Ping received from predecessor

			# If the predecessor is different from the previous, means a node has been added or deleted
			if self.predecessor != msg_sender:
				# Updating predecessor
				self.predecessor = msg_sender
				# Adding previous backup files to current files
				for file in self.backUpFiles:
					if file not in self.files:
						self.files.append(file)

			# Creating a ping response to share information with the predecessor
			response_msg = ["ping_response", self.files, self.successor ,(self.host, self.port)]
			response_msg = dumps(response_msg)
			response_msg = response_msg.encode('utf-8')

			# Adding all the files of the predecessor to our backup, if not already there
			pred_files = msg[1]
			for file in pred_files:
				if file not in self.backUpFiles:
					self.backUpFiles.append(file)

			
			client.send(response_msg)

		else:
			print("got some other message")

	def periodic_ping(self):
		# Function to keep pinging the successor until the node leaves or fails
		while not self.stop:
			# Creating ping mesage
			sock = socket.socket()
			msg = ["ping", self.files ,(self.host, self.port)]
			msg = dumps(msg)
			msg = msg.encode('utf-8')
			try:
				# Trying to connect with successor
				sock.connect(self.successor)
				
			except:
				# The failure of the successor is detected if connection failed. Updating the successor to our backup successor
				self.successor = self.backup_succ
				sock.connect(self.successor)

			# Sending ping
			sock.send(msg)

			# Waiting for a response from the ping
			ping_response = sock.recv(self.transmission_limit).decode()
			ping_response = loads(ping_response)

			succ_files = ping_response[1]
			# Updating current backup according to files that successor has
			for file in succ_files:
				if self.lookup_file(file) and file not in self.files:
					self.files.append(file)
				elif file not in self.backUpFiles:
					self.backUpFiles.append(file)
					
			# Updating backup sucessor, this changes according what the ping sent
			new_backup = tuple(ping_response[2])
			self.backup_succ = new_backup

			time.sleep(0.25)

	def listener(self):
		
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()
	

	def join(self, joiningAddr):
		

		if joiningAddr == "":
			# Only one node
			return 

		# Sending join request to the address we know
		sock = socket.socket()
		sock.connect(joiningAddr)

		msg = dumps(["join", self.key, (self.host,self.port)])
		msg = msg.encode('utf-8')

		sock.send(msg)


	def put(self, fileName):
		if self.stop:
			return

		# If the file belongs with this node
		if self.lookup_file(fileName):
			self.files.append(fileName)
			# add file to directory of this node
			try:
				f = open(fileName, "r")
				content = f.read()
				f.close()

				g = open(self.dir+fileName, "x")
				g.write(content)
				g.close()
			except:
				print("Couldn't open file")
				pass

		else:
			# Forwarding request to the successor
			sock = socket.socket()
			sock.connect(self.successor)
			msg = dumps(["put", fileName, (self.host,self.port)])
			msg = msg.encode('utf-8')
			sock.send(msg)

		
	def get(self, fileName):

		# If current node has the file
		if fileName in self.files:
			return fileName
		
		else:
			# Forwarding request to successor
			sock = socket.socket()
			sock.connect(self.successor)
			msg = dumps(["get", fileName, (self.host,self.port)])
			msg = msg.encode('utf-8')
			sock.send(msg)

			get_response = sock.recv(self.transmission_limit)
			get_response = get_response.decode()
			get_response = loads(get_response)

			if get_response[0] == "Not_found":
				return None
			elif get_response[0] == "get_response":
				# Receive the file and save in current directory.

				# sock2 = socket.socket()
				# file_source = tuple(get_response[-1])
				# sock2.connect(file_source)
				# self.receiveFile(sock2,fileName)

				return fileName
			else:
				print("Should not get anything else")
				return None


	def leave(self):

		# Informing predecessor of us leaving
		sock = socket.socket()
		sock.connect(self.predecessor)

		msg = dumps(["succ_left", self.successor, (self.host,self.port)])
		msg = msg.encode('utf-8')
		sock.send(msg)

		# Informing successor of us leaving
		sock2 = socket.socket()
		sock2.connect(self.successor)

		msg2 = dumps(["pred_left", self.files, self.backUpFiles, (self.host,self.port)])
		msg2 = msg2.encode('utf-8')
		sock2.send(msg2)


		# Sending files to successor

		# for file in self.files:
		# 	file_path = self.dir+"/"+file
		# 	self.sendFile(sock2,file_path)

		# for file in self.backUpFiles:
		# 	file_path = self.dir+"/"+file
		# 	self.sendFile(sock2,file_path)
			
		self.stop = True

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def receiveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()

	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True

		
