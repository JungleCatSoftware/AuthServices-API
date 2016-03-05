CREATE TABLE users (
  username text,
  org text,
  email text,
  parentuser text,
  createdate timestamp,
  salt text,
  hash text,
  PRIMARY KEY (org, username)
);
