from fastapi import APIRouter, Form
from sqlalchemy import text
from core.dependencies import engine, transactions
import pandas as pd
from scipy.optimize import newton
from scipy.optimize import brentq
from datetime import datetime
from decimal import Decimal

router = APIRouter()


@router.post("/portfolio")
def holdings(client_pan: str = Form(...), portfolio: str = Form(...)):

    query = text(
        """
        SELECT
            client_pan, portfolio, transactions.instrument, transactions.instrument_name, folio,

            -- Total
            CAST(SUM(balance_quantity) AS numeric(14,2)) AS holding_quantity,
            CAST(SUM(holding_value) AS numeric(14,2)) AS holding_value,
            ROUND(SUM(holding_value) / NULLIF(SUM(balance_quantity), 0), 2) AS avg_price,
            CAST(eod.current_price AS numeric(14,2)) as current_price,

            /* ========== LONG TERM ========== */
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN balance_quantity ELSE 0 END) AS numeric(14,2)) AS long_term_quantity,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN holding_value ELSE 0 END) as numeric(14,2)) AS long_term_value,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date > 365 THEN balance_quantity ELSE 0 END) * eod.current_price as numeric(14,2)) as long_term_current_value,

            /* ========== SHORT TERM ========== */
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN balance_quantity ELSE 0 END) AS numeric(14,2)) AS short_term_quantity,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN holding_value ELSE 0 END) as numeric(14,2)) AS short_term_value,
            CAST(SUM(CASE WHEN CURRENT_DATE - transaction_date < 365 THEN balance_quantity ELSE 0 END) * eod.current_price as numeric(14,2)) as short_term_current_value

            FROM
                transactions
            INNER JOIN
                eod
            ON transactions.instrument =  eod.instrument
            WHERE
                client_pan = :client_pan
            GROUP BY
                client_pan, portfolio, transactions.instrument, transactions.instrument_name, folio, eod.current_price
            HAVING SUM(balance_quantity) > 0;
        """
    )

    with engine.connect() as connection:
        parms = {"client_pan": client_pan}
        result = connection.execute(query, parms)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    df["long_term_price"] = df.apply(
        lambda row: (
            round(row["long_term_value"] / row["long_term_quantity"], 2)
            if row["long_term_quantity"] > 0
            else 0
        ),
        axis=1,
    )
    df["long_term_pl"] = round(df["long_term_current_value"] - df["long_term_value"], 2)
    df["short_term_price"] = df.apply(
        lambda row: (
            round(row["short_term_value"] / row["short_term_quantity"], 2)
            if row["short_term_quantity"] > 0
            else 0
        ),
        axis=1,
    )
    df["short_term_pl"] = round(
        df["short_term_current_value"] - df["short_term_value"], 2
    )

    df["current_value"] = df["holding_quantity"] * df["current_price"]
    df["pl"] = round(df["current_value"] - df["holding_value"], 2)
    df["plp"] = round((df["pl"] / df["holding_value"]) * 100, 2)

    df["xirr"] = df.apply(
        lambda row: calc_xirr(
            row["client_pan"], row["instrument"], row["folio"], row["current_value"]
        ),
        axis=1,
    )
    df["cagr"] = 0  # Placeholder for CAGR calculation

    df.to_clipboard(index=False)

    if portfolio != "All":
        df = df[df["portfolio"] == portfolio]

    return {
        "status": "success",
        "message": "Mutual fund holdings fetched successfully",
        "data": df.to_dict(orient="records"),
    }


def calc_xirr(client_pan: str, instrument: str, folio: str, current_value: float):
    sql = text(
        """
        SELECT
            transaction_date, holding_value
        FROM transactions
        WHERE
            client_pan = :client_pan AND
            instrument = :instrument AND
            folio = :folio AND
            balance_quantity > 0
        ORDER BY transaction_date
        """
    )
    with engine.connect() as connection:
        params = {"client_pan": client_pan, "instrument": instrument, "folio": folio}
        transactions = pd.read_sql(sql, connection, params=params)
        cashflows = []
        dates = []

    for row in transactions.itertuples():
        cashflows.append(-float(row.holding_value))
        dates.append(row.transaction_date)
    # Add current value as the final cashflow
    cashflows.append(float(current_value))
    dates.append(datetime.now().date())

    xirr_value = xirr(cashflows, dates)
    return round(xirr_value * 100, 2) if xirr_value is not None else None


def xirr(cashflows, dates):
    """Compute XIRR using Brent’s method for stability."""

    if len(set(dates)) == 1:  # All dates are the same, invalid for XIRR
        return None
    # Must have inflow & outflow
    if not (any(cf < 0 for cf in cashflows) and any(cf > 0 for cf in cashflows)):
        return None

    # Convert cashflows to Decimal for precision
    cashflows = [Decimal(cf) for cf in cashflows]

    # Ensure dates are datetime objects
    # dates = [
    #     d if isinstance(d, datetime) else datetime.strptime(d, "%Y-%m-%d")
    #     for d in dates
    # ]

    def npv(rate):
        """Net Present Value function used in Brent’s method."""
        return sum(
            float(cf) / ((1 + rate) ** ((d - dates[0]).days / 365.0))
            for cf, d in zip(cashflows, dates)
        )

    try:
        # Search for the root in a wide range
        return brentq(npv, -0.9999, 100.0)
    except ValueError:
        return None  # No valid root found
