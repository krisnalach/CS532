import requests
import sys
import time

HOST = "http://localhost"
PORT = 8000

def run():
    try:
        champ_name = sys.argv[1]
    except:
        print(f"Please specify champion name")
        sys.exit()
    
    # make GET request for champ_name winrate
    url = f"{HOST}:{PORT}/winrate?champ_name={champ_name}"
    start_t = time.time()
    response = requests.get(url)
    latency = time.time() - start_t

    print(f"Latency: {latency}")
    
    if response.status_code == 200:
        # if successful, print response
        data = response.json()
        print(f"Champion: {data['champ_name']}, Winrate: {data['winrate']}")
    else:
        print(f"Error: {response.status_code}, {response.json()}")

if __name__ == "__main__":
    run()