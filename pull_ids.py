from dotenv import load_dotenv
import os
import requests
import json
import time

API_KEY = os.getenv("API_KEY")

def pull_puuids():
    # ranked tiers range from I to IV
    tiers = ["I", "II", "III", "IV"]
    puuids = []

    for tier in tiers:
        for i in range(1, 10):
            # puuids are stored in pages per rank tier
            # we exclusively look at diamond games for this
            API_URL = f"https://na1.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/DIAMOND/{tier}?page={i}&api_key={API_KEY}"
            response = requests.get(API_URL)

            # store
            for entry in response.json():
                puuids.append(entry["puuid"])
    # write to file
    with open("puuids.txt", "w") as f:
        for puuid in puuids:
            f.write(puuid)
            f.write("\n")

def pull_match_ids():
    # keep as set, since there may be duplicates
    m_ids = set()
    puuids = []
    # read puuids
    with open("puuids.txt", "r") as f:
        for line in f.readlines():
            puuids.append(line.strip())
        
        for puuid in puuids:
            API_URL = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&start=0&count=25&api_key={API_KEY}"
            response = requests.get(API_URL)

            # store
            for m_id in response.json():
                m_ids.add(m_id)

            # limit for now
            if len(m_ids) > 10000:
                break
            
            # so we don't go over our API limits
            time.sleep(1.25)
            
    # write to file
    with open("m_ids.txt", "w") as f:
        for m_id in m_ids:
            f.write(m_id)
            f.write("\n")

if __name__ == "__main__":
    pull_puuids()
    pull_match_ids()