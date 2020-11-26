# DROP TABLES
# --> PostgreSQL reference: https://www.postgresql.org/docs/9.1/datatype-numeric.html

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT
        , start_time TIMESTAMP
        , user_id VARCHAR
        , level VARCHAR 
        , song_id VARCHAR
        , artist_id VARCHAR
        , session_id INT
        , location VARCHAR
        , user_agent TEXT
        , PRIMARY KEY(songplay_id)
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR
    , first_name VARCHAR
    , last_name VARCHAR
    , gender VARCHAR
    , level VARCHAR
    , PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR
    , title VARCHAR
    , artist_id VARCHAR
    , year INT
    , duration DECIMAL
    , PRIMARY KEY (song_id)
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR
    , name VARCHAR
    , location VARCHAR
    , latitude DECIMAL
    , longitude DECIMAL
    , PRIMARY KEY (artist_id)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP
    , hour INT
    , day INT
    , week INT
    , month INT
    , year INT
    , weekday INT
    , PRIMARY KEY (start_time))
""")

# INSERT RECORDS
# Ref.: handlings INSERT conflicts: https://www.postgresqltutorial.com/postgresql-upsert/

songplay_table_insert = ("""
    INSERT INTO songplays (
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(songplay_id) DO UPDATE SET start_time=songplays.start_time, user_id=songplays.user_id, level=songplays.level
        , song_id=songplays.song_id, artist_id=songplays.artist_id, session_id=songplays.session_id, location=songplays.location
        , user_agent=songplays.user_agent
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET first_name=users.first_name, last_name=users.last_name, gender=users.gender, level=users.level
    
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO time (
        start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

# song_select = ("""
#     SELECT *
#     FROM songs
#     WHERE song_id = %s
# """)

# # find the song ID and artist ID based on the title, artist name, and duration of a song.
song_select = ("""
    SELECT tbl_s.song_id
        , tbl_s.artist_id
    FROM songs tbl_s
    JOIN artists tbl_a ON tbl_s.artist_id = tbl_a.artist_id
    WHERE tbl_s.title = %s
        AND tbl_a.name = %s
        AND tbl_s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]