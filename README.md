# Final Project for CS532

First, run
`pip install -r requirements.txt`
preferably in a virtual environment.

To run the player_id and match_id polling, run `python pull_ids.py`
This program writes to puuids.txt and m_ids.txt, filling them both with player_ids and match_ids, respectively.

To run the front-end server, run `python server.py`
Caching is currently enabled.
To run the database, run `python database.py`
The database program is currently configured to repopulate the database, while serving client requests on a separate thread.
After repopulating the v1 dataframe, it creates the v2 dataframe from the v1 dataframe.
It may crash from the df.write.mode("overwrite").parquet(db_path) line.
I overcame this by just deleting the .part files in db/league_db.

To run the client, run `python client.py {champion_name}`
champion_name must be valid, else the system crashes.

Testing is mostly through print statements, including bug testing and latency testing.
E.g: 
	`latency = time.time() - start_t
        print(latency)
        winrate = response.winrate`
