-- One PostgreSQL server for the local stack, one database and one role per
-- service: no service can read another one's tables. In production each
-- database would usually live on its own server.
-- The passwords are development values; never reuse them anywhere else.
CREATE ROLE ledger LOGIN PASSWORD 'ledger-dev-only';
CREATE DATABASE ledger OWNER ledger;
REVOKE ALL ON DATABASE ledger FROM PUBLIC;

CREATE ROLE transfers LOGIN PASSWORD 'transfers-dev-only';
CREATE DATABASE transfers OWNER transfers;
REVOKE ALL ON DATABASE transfers FROM PUBLIC;

CREATE ROLE notifications LOGIN PASSWORD 'notifications-dev-only';
CREATE DATABASE notifications OWNER notifications;
REVOKE ALL ON DATABASE notifications FROM PUBLIC;
