CREATE TABLE userpasswords (
  username text,
  org text,
  hash text,
  hashsequece text,
  PRIMARY KEY (org, username)
);
