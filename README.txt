First, run
pip install -r requirements.txt
preferably in a virtual environment.

To run the player_id and match_id polling, run python pull_ids.py.
This program writes to puuids.txt and m_ids.txt, filling them both with player_ids and match_ids, respectively.

To run the front-end server, run python server.py.
To run the database, run python database.py