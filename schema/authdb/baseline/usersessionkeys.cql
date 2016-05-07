CREATE TABLE usersessionkeys (
  sessionkey text,
  sessionid uuid,
  username text,
  org text,
  PRIMARY KEY (sessionkey, sessionid, username, org)
);
