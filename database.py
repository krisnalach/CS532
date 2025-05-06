from dotenv import load_dotenv
import os, sys, signal
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, BooleanType, StringType
import time
from tqdm import tqdm
import grpc
import database_pb2, database_pb2_grpc

# weird workarounds to get this running on windows
# may need to comment out on macOs/Linux (untested)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = "python" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "python" 
spark = SparkSession.builder.appName("LeagueDB").getOrCreate()

db_path = "db/league_db"
db_path_v2 = "db/league_db_v2"

# schema for version 1 of dataframe, optimized for storage
schema = StructType([
    StructField("match_id", StringType(), False),
    StructField("blue_top", StringType(), False),
    StructField("blue_jg", StringType(), False),
    StructField("blue_mid", StringType(), False),
    StructField("blue_adc", StringType(), False),
    StructField("blue_supp", StringType(), False),
    StructField("red_top", StringType(), False),
    StructField("red_jg", StringType(), False),
    StructField("red_mid", StringType(), False),
    StructField("red_adc", StringType(), False),
    StructField("red_supp", StringType(), False),
    StructField("blue_win", BooleanType(), False)
])

# schema for version 2 of dataframe, optimized for operations
schema_v2 = StructType([
    StructField("match_id", StringType(), False),
    StructField("team", StringType(), False),
    StructField("champion", StringType(), False),
    StructField("role", StringType(), False),
    StructField("win", BooleanType(), False)
])

# read dataframes from file if exist
if os.path.exists(db_path) and len(os.listdir(db_path)) > 0:
    df = spark.read.parquet(db_path)
    print(f"Number of rows in dataframe: {df.count()}")
else:
    df = spark.createDataFrame([], schema)

if os.path.exists(db_path_v2) and len(os.listdir(db_path_v2)) > 0:
    df_v2 = spark.read.parquet(db_path_v2)
    print(f"Number of rows in dataframe: {df_v2.count()}")
else:
    df_v2 = spark.createDataFrame([], schema_v2)

# we are creating WAY too many .part files
spark.conf.set("spark.sql.shuffle.partitions", 100)

load_dotenv()
API_KEY = os.getenv("API_KEY")
MATCH_URL = f"https://americas.api.riotgames.com/lol/match/v5/matches/"

#API_URL = f"{MATCH_URL}?api_key={API_KEY}"

def populate_db():
    """
    Populate the database for the current patch
    """
    # load match_ids
    m_ids = []
    with open("m_ids.txt", "r") as f:
        for line in f.readlines():
            m_ids.append(line.strip())
    
    # limit for now
    m_ids = m_ids[:3000]

    rows_to_add = []

    for m_id in tqdm(m_ids):
        # construct URL
        url = f"{MATCH_URL}{m_id}?api_key={API_KEY}"

        # query the API
        response = requests.get(url)

        #print(f"STATUS: {response.status_code}, {response.reason}")

        #only continue on successful request
        if response.status_code == 200:
            data = response.json()
            success, row = extract_and_write(data, "15.9")
            if success:
                #print(f"Writing: {row}")
                rows_to_add.append(row)
            
        time.sleep(1.25)

    # create spark dataframe, add to existing
    global df
    df_to_add = spark.createDataFrame(rows_to_add, schema)
    df_to_add.show()
    df = df.union(df_to_add).distinct()

    df.write.mode("overwrite").parquet(db_path)
    print("Dataframe updated")

def extract_and_write(data, curr_patch):
    """
    Function to handle json data and write it to a database file
    Returns T, data if successful, F otherwise
    """
    pos_ordering = {
        "TOP": 0,
        "JUNGLE": 1,
        "MIDDLE": 2,
        "BOTTOM": 3,
        "UTILITY": 4
    }
    info = data["info"]
    #print(info["gameVersion"][:4], info["queueId"])
    # check if game ended, if game was ranked solo/duo, and if game was current patch (excluding current patch for now)
    if info["endOfGameResult"] == "GameComplete" and info["queueId"] == 420: #and info["gameVersion"][:4] == curr_patch:
        m_id = data["metadata"]["matchId"]
        blue_info = []
        red_info = []
        for p in info["participants"]:
            # really weird edge case
            # if a player is AFK, then "teamPosition" = ""
            try:
                pos_ordering[p["teamPosition"]]
            except KeyError as e:
                print(e)
                # just discard the match if this happens, plenty of matches exist (and these games skew winrates)
                return (False, None)
            if p["teamId"] == 100:
                blue_info.append((p["championName"], pos_ordering[p["teamPosition"]]))
                blue_win = p["win"]
            else:
                red_info.append((p["championName"], pos_ordering[p["teamPosition"]]))
        
        # enforce strict order for columns
        blue_sorted = sorted(blue_info, key=lambda x: x[1])
        blue_champs = [e[0] for e in blue_sorted]

        red_sorted = sorted(red_info, key=lambda x: x[1])
        red_champs = [e[0] for e in red_sorted]

        # create row with columns
        # match_id, blue_top, blue_jg, blue_mid, blue_adc, blue_supp, red_top, red_jg, red_mid, red_adc, red_supp, blue_win
        return (True, [m_id] + blue_champs + red_champs + [blue_win])
    return (False, None)

def create_df_v2():
    """
    Function to create the new v2 dataframe from an existing dataframe
    rather than pulling data again when it's really slow
    """
    blue_pos = ["blue_top", "blue_jg", "blue_mid", "blue_adc", "blue_supp"]
    red_pos = ["red_top", "red_jg", "red_mid", "red_adc", "red_supp"]

    # convert blue_win to win, and split so we have proper team columns
    blue_df = df.select("match_id", *blue_pos, "blue_win").withColumn("win", f.col("blue_win")).withColumn("team", f.lit("blue"))
    red_df = df.select("match_id", *red_pos, "blue_win").withColumn("win", ~f.col("blue_win")).withColumn("team", f.lit("red"))

    # explode on roles to get just champions
    blue_v2 = blue_df.select("match_id", "team", f.explode(f.array(*blue_pos)).alias("champion"), "win")
    red_v2 = red_df.select("match_id", "team", f.explode(f.array(*red_pos)).alias("champion"), "win")

    df_v2 = blue_v2.union(red_v2)
    df_v2.show(truncate=False)
    
    df_v2.write.mode("overwrite").parquet("db/league_db_v2")

class Database(database_pb2_grpc.DatabaseServicer):
    def Winrate(self, request, context):
        champ_name = request.champ_name
        version = request.version

        if version == "v2":
            # calculation on version 2 of dataframe
            # filter based on champ name
            champ_df = df_v2.filter(f.col("champion") == champ_name)
            # filter based on if win
            win_df = champ_df.filter(f.col("win") == True)

            # simply divide counts to get winrate
            return database_pb2.WinrateReply(winrate=(win_df.count()/champ_df.count()))
        else:
            # calculation on version 1 of dataframe

            # need to union/filter between all 5 roles for each team
            blue_champ_df = df.filter(f.col("blue_top") == champ_name) \
                .union(df.filter(f.col("blue_jg") == champ_name)) \
                .union(df.filter(f.col("blue_mid") == champ_name)) \
                .union(df.filter(f.col("blue_adc") == champ_name)) \
                .union(df.filter(f.col("blue_supp") == champ_name))
            
            red_champ_df = df.filter(f.col("red_top") == champ_name) \
                .union(df.filter(f.col("red_jg") == champ_name)) \
                .union(df.filter(f.col("red_mid") == champ_name)) \
                .union(df.filter(f.col("red_adc") == champ_name)) \
                .union(df.filter(f.col("red_supp") == champ_name))
            
            # again, divide by counts to get winrate
            n = blue_champ_df.count() + red_champ_df.count()
            wins = blue_champ_df.filter(f.col("blue_win") == True).count() + red_champ_df.filter(f.col("blue_win") == False).count()

            return database_pb2.WinrateReply(winrate=(wins / n))

def serve():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_DatabaseServicer_to_server(Database(), server)
    server.add_insecure_port(f"[::]:{8080}")
    server.start()
    print(f"Server is running on port {8080}")
    server.wait_for_termination()

if __name__ == "__main__":
    server_thread = threading.Thread(target=serve, daemon=False)
    server_thread.start()

    while df.count() < 3000:
        populate_db()

    create_df_v2()