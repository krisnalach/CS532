from flask import Flask, request, jsonify
import database_pb2, database_pb2_grpc
import grpc
from collections import deque
import time

HOST = "localhost"
PORT = 8000

app = Flask(__name__)

class lru_cache():
    # least recently used cache
    def __init__(self, n):
        self.cache = deque(maxlen=n)
        self.maxlen = n
    
    # add champ_info to cache
    def add(self, champ_info):
        self.cache.append(champ_info)

    # read from the cache
    # and update LRU cache
    def get(self, champ_name):
        for champ_info in self.cache:
            if champ_info[0] == champ_name:
                self.evict(champ_name)
                self.add(champ_info)
                return champ_info
        # return -1 if doesn't exist
        return -1
    # remove specific element from cache
    def evict(self, champ_name):
        remove_i = -1
        for i, champ_info in enumerate(self.cache):
            if champ_info[0] == champ_name:
                remove_i = i
        # element is in cache
        if remove_i != -1:
            self.cache = deque([champ_info for i, champ_info in enumerate(self.cache) if i != remove_i], maxlen=self.maxlen)
    
    def print_content(self):
        for champ_info in self.cache:
            print(champ_info)

cache = lru_cache(n=10)

@app.route('/winrate', methods=['GET'])
def get_winrate():
    champ_name = request.args.get('champ_name', None)
    
    if champ_name is None:
        return jsonify({"error": "Champion name is required"}), 400
    
    #champ_info = cache.get(champ_name)
    #if champ_info != -1:
        winrate = champ_info[1]
    #else:
    # communicate with database to get champion winrate
    with grpc.insecure_channel(f"localhost:{8080}") as channel:
        stub = database_pb2_grpc.DatabaseStub(channel)
        start_t = time.time()
        response = stub.Winrate(database_pb2.WinrateRequest(champ_name=champ_name, version="v2"))
        latency = time.time() - start_t
        print(latency)
        winrate = response.winrate
        # add champ, winrate to lru cach
        #cache.add([champ_name, winrate])
    
    #cache.print_content()

    return jsonify({"champ_name": champ_name, "winrate": winrate})

if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)