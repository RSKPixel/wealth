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

            -- Total
            SUM(balance_quantity) AS holding_quantity,
            SUM(holding_value) AS holding_value,
            ROUND(SUM(holding_value) / NULLIF(SUM(balance_quantity), 0), 2) AS avg_price,
            mutualfund_eod.nav as current_price,

            /* ========== LONG TERM ========== */
            SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN balance_quantity ELSE 0 END) AS long_term_quantity,
            SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN holding_value ELSE 0 END) AS long_term_value,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN balance_quantity ELSE 0 END) * mutualfund_eod.nav as NUMERIC(14,2)) as long_term_cv,

            /* ========== SHORT TERM ========== */
            SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN balance_quantity ELSE 0 END) AS short_term_quantity,
            SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN holding_value ELSE 0 END) AS short_term_value,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN balance_quantity ELSE 0 END) * mutualfund_eod.nav as NUMERIC(14,2)) as short_term_cv

            FROM
                transactions
            INNER JOIN
                mutualfund_eod
            ON transactions.instrument =  mutualfund_eod.isin
            WHERE
                client_pan = :client_pan
            GROUP BY
                client_pan, instrument, instrument_name, folio, mutualfund_eod.nav
            HAVING SUM(balance_quantity) > 0;
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
