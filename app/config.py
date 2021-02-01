
import os
from dotenv import load_dotenv, find_dotenv


"""
Config file
"""

load_dotenv(find_dotenv(), verbose=True)


config = dict()
config.update(dict(
    version="1.1.0",
    loglevel="INFO",
    DB=os.getenv('DB'),
    DBHOST=os.getenv('DBHOST'),
    DBPORT=os.getenv('DBPORT'),
    N_RECONNECT_TRIES=5,
    RECONNECT_DELAY=1.0, #s
    baseurl='http://localhost:8989'
))


if __name__ == "__main__":
    print(config)
