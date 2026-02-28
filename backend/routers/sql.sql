CREATE TABLE wealth_transactions (
    id                  SERIAL          NOT NULL,
    client_pan          VARCHAR(12)     NOT NULL,
    portfolio           VARCHAR(50)     NOT NULL,
    asset_class         VARCHAR(50)     NOT NULL,
    folio_id            VARCHAR(50),
    folio_name          VARCHAR(100),
    instrument          VARCHAR(50)     NOT NULL,
    instrument_name     VARCHAR(100)    NOT NULL,
    transaction_date    DATE,
    transaction_id      VARCHAR(50),
    transaction_type    VARCHAR(20)     NOT NULL,
    quantity            NUMERIC(15,4),
    price               NUMERIC(15,4),
    value               NUMERIC(15,4),
    PRIMARY KEY (id)
)