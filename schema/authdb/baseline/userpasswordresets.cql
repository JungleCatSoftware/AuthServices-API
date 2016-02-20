CREATE TABLE userpasswordresets (
  resetid uuid,
  email text,
  requestdate timestamp,
  PRIMARY KEY (resetId)
);
