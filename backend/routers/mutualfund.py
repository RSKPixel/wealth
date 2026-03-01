from fastapi import APIRouter
from sqlalchemy import text
from core.dependencies import engine, transactions
import pandas as pd

router = APIRouter()


@router.post("/holdings")
def holdings(client_pan: str):

    query = text(
        """
        SELECT instrument, instrument_name, folio, sum(quantity) as holding_quantity
        FROM transactions
        WHERE client_pan = :client_pan
        GROUP BY instrument, instrument_name, folio
        HAVING sum(quantity) > 0
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query, {"client_pan": client_pan})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return {
        "status": "success",
        "message": "Helloworld from mutual fund router",
        "data": df.to_dict(orient="records"),
    }
