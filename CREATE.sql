create table if not exists Artists (
  id serial primary key,
  name varchar(30) not null
);

create table if not exists Artists_albums (
  artist_id integer NOT NULL,
  album_id integer NOT NULL,
  FOREIGN KEY (artist_id) REFERENCES Artists (id),
  FOREIGN KEY (album_id) REFERENCES Albums (id),
  UNIQUE (artist_id, album_id)
);

create table if not exists Albums (
  id serial primary key,
  album_name varchar(30) not null,
  year integer not null
);

create table if not exists Tracks (
  id serial primary key,
  track_name varchar(30) not null,
  album_id integer references Albums(id),
  track_length integer not null
);

create table if not exists Tracks_lists (
  track_id integer NOT NULL,
  playlist_id integer NOT NULL,
  FOREIGN KEY (track_id) REFERENCES Tracks(id),
  FOREIGN KEY (playlist_id) REFERENCES Playlist(id),
  UNIQUE (track_id, playlist_id)
);

create table if not exists Playlist (
  id serial primary key,
  name varchar(30) not null,
  year integer not null
);

create table if not exists Artists_genre (
  artist_id integer NOT NULL,
  genre_id integer NOT NULL,
  FOREIGN KEY (artist_id) REFERENCES Artists(id),
  FOREIGN KEY (genre_id) REFERENCES Genre(id),
  UNIQUE (artist_id, genre_id)
);

create table if not exists Genre (
  id serial primary key,
  name varchar(30) not null
);
