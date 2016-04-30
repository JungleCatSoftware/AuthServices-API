CREATE TABLE usersessions (
  username text,
  org text,
  sessionid uuid,
  startdate timestamp,
  lastupdate timestamp,
  PRIMARY KEY ((username,org), sessionid)
);
