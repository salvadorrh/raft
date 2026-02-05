import grpc
from concurrent import futures
import subprocess
import os, sys, shutil
import raft_pb2
import raft_pb2_grpc

import configparser
from pathlib import Path


class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self.max_servers = 5 # from assigment 1
        self.servers = [None, None, None, None, None] # array of alive/dead servers
    
    def StartRaft(self, request, context):
        # Start request.arg servers
        # Return Reply(wrongLeader=False) on success
        N = request.arg # num servers

        # Checking for out of bounds
        if N <= 0 or N > self.max_servers:
            return raft_pb2.Reply(wrongLeader=False, error="N should be more than or equal to 0 and less than num of servers")

        # Clean up
        for i in range(N):
            path = f"./state/server{i}"
            if os.path.exists(path):
                shutil.rmtree(path)

        for i in range(N):
            # sys.executable to use the correct interpreter
            self.servers[i] = subprocess.Popen([sys.executable , "server.py", f"{i}"])

        return raft_pb2.Reply(wrongLeader=False, error="")


    def StartServer(self, request, context):
        idx = request.arg
        p = self.servers[idx]
        if p is None or p.poll() is not None:
            self.servers[idx] = subprocess.Popen([sys.executable, "server.py", f"{idx}"])
        return raft_pb2.Reply(wrongLeader=False, error="")

    # Read config.ini and get server data
    def get_active_servers_from_config(self):
        config = configparser.ConfigParser()
        # path to config.ini in the same directory as this file
        config_path = Path(__file__).parent / "config.ini"
        config.read(config_path)
        active_raw = config["Servers"]["active"]
        active_servers = [int(x.strip()) for x in active_raw.split(",")]
        return active_servers

    # Tis should hit the servers in find_available_servers and return True
    # If the server is active
    # only one server may be active so we need to ping every server
    def ping_server(self, server_id):
        try:
            port = 9001 + server_id
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            stub.ping(raft_pb2.Empty(), timeout=1.0)
            return True
        except grpc.RpcError:
            return False
        
    
    def find_available_server(self):
        active_servers = self.get_active_servers_from_config() # find which servers are active in ini file
        for server_id in active_servers:
            if self.ping_server(server_id): 
                return server_id # return server that is ALIVE 
        return None
        
    # (GET)
    def forward_get_to_server(self, server_id, request_key):
        port = 9001 + server_id
        channel = grpc.insecure_channel(f"localhost:{port}")
        try: 
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            kv = stub.Get(raft_pb2.StringArg(arg=request_key), timeout=1.0)
            return True, kv.value
        except grpc.RpcError:
            return False, ""

    # (PUT)
    def forward_put_to_server(self, server_id, request_key, request_value):
        port = 9001 + server_id
        channel = grpc.insecure_channel(f"localhost:{port}")
        try:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            stub.Put(raft_pb2.KeyValue(key=request_key, value=request_value), timeout=1.0)
            return True
        except grpc.RpcError:
            return False
    
    # Read a value for a key (given)
    def Get(self, request, context):
        server_id = self.find_available_server()
        if server_id is None:
            print("a\n")
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")
        
        # Forward to server
        success, value = self.forward_get_to_server(server_id, request.key)
        if success:
            print("b\n")
            return raft_pb2.Reply(wrongLeader=False, value=value)
        else:
            print("c\n")
            return raft_pb2.Reply(wrongLeader=True, error="Server error")
    
    # Store a value for a key
    def Put(self, request, context):
        server_id = self.find_available_server()
        if server_id is None:
            print("a\n")
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")
        
        # Forward to server (we just care about success)
        success = self.forward_put_to_server(server_id, request.key, request.value)
        if success:
            print("b\n")
            return raft_pb2.Reply(wrongLeader=False)
        else:
            print("c\n")
            return raft_pb2.Reply(wrongLeader=True, error="Server error")

# Start server on port 8001
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port("[::]:8001")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()