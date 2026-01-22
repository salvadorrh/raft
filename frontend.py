import grpc
from concurrent import futures
import subprocess
import os, sys, shutil
import raft_pb2
import raft_pb2_grpc

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
        if p is not None and p.poll() is None:
            self.servers[idx] = subprocess.Popen([sys.executable, "server.py", f"{idx}"])
        return raft_pb2.Reply(wrongLeader=False, error="")
    
    def Get(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")
    
    def Put(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")

# Start server on port 8001
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port("[::]:8001")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()