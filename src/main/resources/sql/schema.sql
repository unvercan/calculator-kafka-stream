CREATE TABLE IF NOT EXISTS "operation"
(
    "code" INTEGER     NOT NULL,
    "name" VARCHAR(15) NOT NULL,
    PRIMARY KEY ("code"),
    UNIQUE ("code")
);

CREATE TABLE IF NOT EXISTS "calculation"
(
    "id"             UUID    NOT NULL DEFAULT RANDOM_UUID(),
    "first"          INTEGER NOT NULL,
    "second"         INTEGER NOT NULL,
    "operation_code" INTEGER NOT NULL,
    "result"         FLOAT            DEFAULT NULL,
    PRIMARY KEY ("id"),
    FOREIGN KEY ("operation_code") REFERENCES "operation"
);
