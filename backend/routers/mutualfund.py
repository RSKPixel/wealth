from fastapi import APIRouter, Form
from sqlalchemy import text
from core.dependencies import engine, transactions
import pandas as pd

router = APIRouter()


@router.post("/holdings")
def holdings(client_pan: str = Form(...)):

    query = text(
        """
        SELECT
            client_pan, instrument, instrument_name, folio,
            sum(balance_quantity) as holding_quantity,
            sum(holding_value) as holding_value,
            round(sum(holding_value) / sum(balance_quantity), 2) as avg_price
        FROM
            transactions
        WHERE
            client_pan = :client_pan
        GROUP BY
            client_pan, instrument, instrument_name, folio
        HAVING
            sum(balance_quantity) > 0
        """
    )

    with engine.connect() as connection:
        parms = {"client_pan": client_pan}
        result = connection.execute(query, parms)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return {
        "status": "success",
        "message": "Mutual fund holdings fetched successfully",
        "data": df.to_dict(orient="records"),
    }

CREATE TABLE holdings (
    id                 SERIAL PRIMARY KEY,
    client_pan         VARCHAR(10)     NOT NULL,
    instrument         VARCHAR(20)     NOT NULL,
    instrument_name    VARCHAR(255)    NOT NULL,
    folio              VARCHAR(50)     NOT NULL,
    holding_quantity   NUMERIC(15, 3) NOT NULL,
    holding_value      NUMERIC(15, 2)  NOT NULL,
    avg_price          NUMERIC(15, 2)  NOT NULL,
    latest_price       NUMERIC(15, 2),
    current_value      NUMERIC(15, 2),
    value_date         DATE,
    pl                 NUMERIC(15, 2),
    plp                NUMERIC(5, 2),
    xirr               NUMERIC(5, 2),
    cagr               NUMERIC(5, 2),
);