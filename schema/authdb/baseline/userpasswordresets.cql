CREATE TABLE userpasswordresets (
  username text,
  org text,
  resetid uuid,
  requestdate timestamp,
  PRIMARY KEY ((username,org))
);
