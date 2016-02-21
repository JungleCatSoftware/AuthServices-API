CREATE TABLE users (
  username text,
  org text,
  email text,
  parentuser text,
  createdate timestamp,
  PRIMARY KEY (org, username)
);
