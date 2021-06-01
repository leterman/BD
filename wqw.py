import sqlalchemy
import psycopg2
from sqlalchemy.engine import Inspector

db = ('postgresql://userok:12345@localhost:5432/bass')
engine = sqlalchemy.create_engine(db)
connection = engine.connect()

sel1 = connection.execute("""SELECT name, COUNT(artist_id)
FROM genre
JOIN artists_genre ON genre.id = artists_genre.genre_id
GROUP BY name;
""").fetchmany(10)
print(sel1)

sel2 = connection.execute("""SELECT albums.year, COUNT(tracks.album_id)
FROM albums
LEFT JOIN tracks ON albums.id = tracks.album_id
WHERE albums.year >= 2019 and albums.year <= 2020
GROUP BY albums.year;
""").fetchmany(10)
print(sel2)

sel3 = connection.execute("""SELECT albums.album_name, AVG(tracks.track_length)
FROM albums
LEFT JOIN tracks ON albums.id = tracks.album_id
GROUP BY albums.album_name
ORDER BY AVG(tracks.track_length);
""").fetchmany(10)
print(sel3)

sel4 = connection.execute("""SELECT DISTINCT artists.name
FROM artists
LEFT JOIN artists_albums ON artists.id = artists_albums.artist_id
LEFT JOIN albums ON artists_albums.album_id = albums.id
WHERE NOT albums.year = 2020;
""").fetchmany(10)
print(sel4)

sel5 = connection.execute("""SELECT DISTINCT playlist.name
FROM playlist
LEFT JOIN tracks_lists ON playlist.id = tracks_lists.playlist_id
LEFT JOIN tracks ON tracks_lists.track_id = tracks.id
LEFT JOIN albums ON tracks.album_id = albums.id
LEFT JOIN artists_albums ON albums.id = artists_albums.album_id
LEFT JOIN artists ON artists_albums.artist_id = artists.id
WHERE artists.name LIKE '%%Mozart%%'
ORDER BY playlist.name;
""").fetchmany(10)
print(sel5)

sel6 = connection.execute("""SELECT albums.album_name
FROM albums
LEFT JOIN artists_albums ON albums.id = artists_albums.album_id
LEFT JOIN artists ON artists_albums.artist_id = artists.id
LEFT JOIN artists_genre ON artists.id = artists_genre.artist_id
LEFT JOIN genre ON artists_genre.genre_id = genre.id
GROUP BY albums.album_name
HAVING COUNT(DISTINCT genre.name) >= 2
ORDER BY albums.album_name;
""").fetchmany(10)
print(sel6)

sel7 = connection.execute("""SELECT tracks.track_name
FROM tracks
LEFT JOIN tracks_lists ON tracks.id = tracks_lists.track_id
WHERE tracks_lists.track_id IS NULL;
""").fetchmany(10)
print(sel7)

sel8 = connection.execute("""SELECT artists.name, tracks.track_length
FROM tracks
LEFT JOIN albums ON tracks.album_id = albums.id
LEFT JOIN artists_albums ON albums.id = artists_albums.album_id
LEFT JOIN artists ON artists_albums.artist_id = artists.id
GROUP BY artists.name, tracks.track_length
HAVING tracks.track_length = (SELECT MIN(track_length) FROM tracks)
ORDER BY artists.name;
""").fetchmany(10)
print(sel8)

sel9 = connection.execute("""SELECT DISTINCT albums.album_name
FROM albums
LEFT JOIN tracks ON albums.id = tracks.album_id
WHERE tracks.album_id IN (SELECT album_id from tracks GROUP BY album_id HAVING COUNT(id) = (SELECT COUNT(id) FROM tracks GROUP BY album_id ORDER BY COUNT LIMIT 1))
ORDER BY albums.album_name;
""").fetchmany(10)
print(sel9)