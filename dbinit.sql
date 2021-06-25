SET sql_safe_updates = FALSE;

DROP DATABASE IF EXISTS bank CASCADE;
CREATE DATABASE IF NOT EXISTS bank;

USE bank;

CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    balance INT8
);