# Data-Modeling-with-Postgre
In this project, I apply Data Modeling with Postgres and build an ETL pipeline using Python.
The data is used in this project as format JSON file include song data and log data.
## ** Dataset
Song dataset is a subset of [Million Song Dataset] (http://millionsongdataset.com/)

Sample Record:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
Log dataset is generated by [Event Simulator](https://github.com/Interana/eventsim).

Sample Record:
```
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}
```

## Schema

### Fact Table
**songplays** - A table records in log data associated with song plays i.e. records with page `Next Song`

```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

### Dimension Tables

**users** - users in the app

```
user_id, first_name, last_name, gender, level
```

**songs** - songs in music db

```
song_id, title, artist_id, year, duration
```

**artists** - artists in music db

```
artist_id, name, location, latitude, longitude
```

**time** - timestamp of records in **songplays** broken down into specific units

```
start_time, hour, day, week, month, year, weekday
```
