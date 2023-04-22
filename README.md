# VTA Coding Challenge

My source code for the VTA coding challenge ([https://github.com/vta/coding-challenge#instructions](https://github.com/vta/coding-challenge#instructions)).

When you run the script, a `vta-gtfs-rt.sqlite` file will be put into an `./instance` directory containing the data from the Swiftly API.

I deployed this project using the serverless framework. The API endpoint I created can be found here: [https://a27y33tfw0.execute-api.us-east-1.amazonaws.com/real-time/trip-updates](https://a27y33tfw0.execute-api.us-east-1.amazonaws.com/real-time/trip-updates). Note that this endpoint accesses a remote MySQL Amazon RDS database (not the local SQLite database) which can be updated by the script with the `-r` flag. Read the "Usage" section below to learn more about `-r` and other arguments.

If you have any questions or problems running the program email me (matic@likar.me) and I'll respond as soon as possible! I put a lot of work into the project and it works on my laptop so it would be a shame if anything went wrong.

## Libraries

This project is written in Python3. It uses the following libraries which need to be installed in order to run the program:

`pip3 install requests` (used to access the provided API)

`pip3 install Flask` (used to create the endpoint)

`pip3 install Flask-SQLAlchemy` (used to make use of ORM)

`pip3 install pymysql` (used to connect to an Amazon RDS MySQL database)

I believe no additional libraries are needed since the rest which are used should come with python by default. Make sure to use `python3` when running the program.

## Usage
```
usage: main.py [-h] [-f FORMAT] [-k KEY] [-v VERBOSITY] [-r]

VTA Coding Challenge

optional arguments:
  -h, --help            show this help message and exit
  -f FORMAT, --format FORMAT
                        Format. Only JSON supported. JSON is default
  -k KEY, --key KEY     API key. 59af72683221a1734f637eae7a7e8d9b is default
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity level. Only 0, 1, and 2 supported. Level 1
                        is default with a few custom messages. Level 2 shows
                        all SQL commands being run
  -r, --remote          Flag to use remote MySQL database hosted on Amazon RDS
                        instead of default local SQLite database. Use this
                        option to update the remote database so you can get
                        up-to-date data from the endpoint
```

## Development Notes

- For retrieving the data from the Swiftly API, I used the `requests` python library
- I then used the `Flask-SQLAlchemy` library to create an SQLite database using the library's ORM features. It consists of two tables: `TripUpdate` and `StopTimeUpdate`
- Initially, I used the simplest `SQLAlchemy` functions to insert and update the data from the API into the tables. You can see this code in [old commits such as this](https://github.com/maticstric/vta-coding-challenge/blob/0c38766f4026fb8b20771abca0d526c6667d12c7/main.py)
- Next, used the `Flask` library to create an API endpoint
- I then added some command line arguments to make the program easier to use
- Since I also wanted to deploy this project, I switched to using a remote database. I chose to use a MySQL Amazon RDS instance
- After switching to a remote database, I realized the inserts and updates as I wrote them earlier were far too slow, taking almost 30 minutes to complete. After _a lot_ of optimization trial and error, I settled on a strategy outlined in [this Stack Overflow answer](https://stackoverflow.com/questions/41870323/sqlalchemy-bulk-update-strategies/41882026#41882026). This reduced the total time to insert and update down to only a few seconds at the cost of making the code slightly less readable. I remedied this with many comments
- I then finally deployed this project using the serverless framework [here](https://a27y33tfw0.execute-api.us-east-1.amazonaws.com/real-time/trip-updates)
