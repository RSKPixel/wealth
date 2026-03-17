from fastapi import APIRouter, Form
from sqlalchemy import text
from core.dependencies import engine, transactions
import pandas as pd
from scipy.optimize import newton
from scipy.optimize import brentq
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import sessionmaker

router = APIRouter()


@router.post("/portfolio")
def portfolio(client_pan: str = Form(...), portfolio: str = Form(...)):

    holdings_data, summary_data = holdings(client_pan, portfolio)
    (progress_data, asset_allocation_data, progress_ac_data) = progress(
        client_pan, portfolio
    )

    return {
        "status": "success",
        "message": "Portfolio data fetched successfully",
        "data": {
            "holdings": holdings_data,
            "summary": summary_data,
            "progress": progress_data,
            "asset_allocation": asset_allocation_data,
            "progress_ac": progress_ac_data,
        },
    }


def progress(client_pan: str, portfolio: str):

    start_date = "2022-01-01"
    end_date = datetime.now().date()
    range = pd.date_range(
        start=start_date, end=end_date + pd.offsets.MonthEnd(0), freq="ME"
    )
    progress = pd.DataFrame(
        columns=["date", "invested_value", "current_value", "pl", "plp"]
    )
    progrss_ac = pd.DataFrame(
        columns=[
            "date",
            "asset_class",
            "invested_value",
            "current_value",
            "iv_percentage",
            "cv_percentage",
        ]
    )
    for tdate in range:
        sql = text(
            f"""
                SELECT
                    eod.asset_class,
                    transactions.instrument,
                    transactions.instrument_name,
                    CAST(SUM(holding_value) AS NUMERIC(14,2)) AS invested_value,
                    CAST(SUM(balance_quantity) AS NUMERIC(14,2)) AS invested_quantity,
                    CAST(historical.close_price AS NUMERIC(14,2)) AS close_price,
                    CAST(historical.close_price * SUM(balance_quantity) AS NUMERIC(14,2)) AS current_value
                FROM
                    transactions
                INNER JOIN
                    historical
                ON
                    historical.date = '{tdate}' AND
                    transactions.instrument = historical.instrument
                INNER JOIN
                    eod
                ON
                    transactions.instrument = eod.instrument
                WHERE
                    transactions.transaction_date <= '{tdate}' AND
                    (:portfolio = 'All' OR transactions.portfolio = :portfolio) AND
                    client_pan = :client_pan
                GROUP BY
                    transactions.instrument,
                    transactions.instrument_name,
                    historical.close_price,
                    eod.asset_class
                HAVING SUM(balance_quantity) > 0
            """
        )
        with engine.connect() as connection:
            params = {"portfolio": portfolio, "client_pan": client_pan}
            result = connection.execute(sql, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        numeric_cols = [
            "invested_value",
            "invested_quantity",
            "close_price",
            "current_value",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = tdate

        grouped = (
            df.groupby("date")
            .agg({"invested_value": "sum", "current_value": "sum"})
            .reset_index()
        )
        grouped["pl"] = round(grouped["current_value"] - grouped["invested_value"], 2)
        grouped["plp"] = round((grouped["pl"] / grouped["invested_value"]) * 100, 2)

        grouped_ac = (
            df.groupby(["date", "asset_class"])
            .agg({"invested_value": "sum", "current_value": "sum"})
            .reset_index()
        )

        grouped_ac["iv_percentage"] = round(
            (
                grouped_ac["invested_value"]
                / grouped_ac.groupby("date")["invested_value"].transform("sum")
            )
            * 100,
            2,
        )
        grouped_ac["cv_percentage"] = round(
            (
                grouped_ac["current_value"]
                / grouped_ac.groupby("date")["current_value"].transform("sum")
            )
            * 100,
            2,
        )

        grouped_ac_pivot = grouped_ac.pivot(
            index="date",
            columns="asset_class",
            values=["iv_percentage", "cv_percentage"],
        )

        if progress.empty:
            progress = grouped
            # progress = progress.merge(grouped_ac_pivot, on="date", how="left")
        else:
            progress = pd.concat([progress, grouped], ignore_index=True)
            # progress = progress.merge(grouped_ac_pivot, on="date", how="left")

    progress.sort_values(by="date", inplace=True)
    progress["peak"] = progress["plp"].cummax()
    progress["drawdown"] = progress["plp"] - progress["peak"]
    progress = progress.round(
        {"invested_value": 2, "current_value": 2, "pl": 2, "plp": 2, "drawdown": 2}
    )

    sql = text(
        """
        SELECT
            asset_class,
            CAST(SUM(holding_value) as NUMERIC(14,2)) as holding_value,
            CAST(SUM(current_value) as NUMERIC(14,2)) as current_value
        FROM
            portfolio
        WHERE
            client_pan = :client_pan AND
            (:portfolio = 'All' OR portfolio.portfolio = :portfolio)
        GROUP BY asset_class;
        """
    )
    with engine.connect() as connection:
        params = {"client_pan": client_pan, "portfolio": portfolio}
        result = connection.execute(sql, params)
        asset_allocation = pd.DataFrame(result.fetchall(), columns=result.keys())

    numeric_cols = ["holding_value", "current_value"]
    for col in numeric_cols:
        asset_allocation[col] = pd.to_numeric(asset_allocation[col], errors="coerce")

    total_current_value = asset_allocation["current_value"].sum()
    total_holding_value = asset_allocation["holding_value"].sum()
    asset_allocation["hvp"] = round(
        (asset_allocation["holding_value"] / total_holding_value) * 100, 2
    )
    asset_allocation["cvp"] = round(
        (asset_allocation["current_value"] / total_current_value) * 100, 2
    )
    asset_allocation.sort_values(by="cvp", ascending=False, inplace=True)

    return (
        progress.to_dict(orient="records"),
        asset_allocation.to_dict(orient="records"),
        progrss_ac.to_dict(orient="records"),
    )


def holdings(client_pan: str, portfolio: str):

    query = text(
        """
        SELECT
            client_pan, portfolio, transactions.instrument, transactions.instrument_name, folio,

            -- Total
            CAST(SUM(balance_quantity) AS numeric(14,2)) AS holding_quantity,
            CAST(SUM(holding_value) AS numeric(14,2)) AS holding_value,
            ROUND(SUM(holding_value) / NULLIF(SUM(balance_quantity), 0), 2) AS avg_price,
            CAST(eod.current_price AS numeric(14,2)) as current_price,
            eod.date as current_price_date,
            eod.asset_class as asset_class,

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
            ON
                transactions.instrument = eod.instrument
            WHERE
                client_pan = :client_pan
            GROUP BY
                client_pan, portfolio, transactions.instrument, transactions.instrument_name, folio, eod.current_price, eod.date, eod.asset_class
            HAVING SUM(balance_quantity) > 0;
        """
    )

    with engine.connect() as connection:
        parms = {"client_pan": client_pan}
        result = connection.execute(query, parms)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    numeric_cols = [
        "holding_quantity",
        "holding_value",
        "avg_price",
        "current_price",
        "long_term_quantity",
        "long_term_value",
        "long_term_current_value",
        "short_term_quantity",
        "short_term_value",
        "short_term_current_value",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["long_term_price"] = df.apply(
        lambda row: (
            round(row["long_term_value"] / row["long_term_quantity"], 2)
            if row["long_term_quantity"] > 0
            else 0
        ),
        axis=1,
    )
    df["long_term_pl"] = df["long_term_current_value"] - df["long_term_value"]
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
    df["fv_5y"] = round(df["current_value"] * ((1 + (df["xirr"] / 100)) ** 5), 2)
    df["fv_10y"] = round(df["current_value"] * ((1 + (df["xirr"] / 100)) ** 10), 2)
    df["fv_15y"] = round(df["current_value"] * ((1 + (df["xirr"] / 100)) ** 15), 2)
    df["cagr"] = 0  # Placeholder for CAGR calculation

    Session = sessionmaker(bind=engine)
    session = Session()

    for _, row in df.iterrows():
        sql = text(
            """
            INSERT INTO portfolio (
                client_pan, portfolio, instrument, instrument_name, folio,
                holding_quantity, holding_value, avg_price, current_price, current_price_date,
                long_term_quantity, long_term_value, long_term_current_value,
                short_term_quantity, short_term_value, short_term_current_value,
                long_term_price, long_term_pl, short_term_price, short_term_pl,
                current_value, pl, plp, xirr, cagr, asset_class
            ) VALUES (
                :client_pan, :portfolio, :instrument, :instrument_name, :folio,
                :holding_quantity, :holding_value, :avg_price, :current_price, :current_price_date,
                :long_term_quantity, :long_term_value, :long_term_current_value,
                :short_term_quantity, :short_term_value, :short_term_current_value,
                :long_term_price, :long_term_pl, :short_term_price, :short_term_pl,
                :current_value, :pl, :plp, :xirr, :cagr, :asset_class
            )
            ON CONFLICT (client_pan, folio, instrument) DO UPDATE SET
                holding_quantity = EXCLUDED.holding_quantity,
                holding_value = EXCLUDED.holding_value,
                avg_price = EXCLUDED.avg_price,
                current_price = EXCLUDED.current_price,
                current_price_date = EXCLUDED.current_price_date,
                long_term_quantity = EXCLUDED.long_term_quantity,
                long_term_value = EXCLUDED.long_term_value,
                long_term_current_value = EXCLUDED.long_term_current_value,
                short_term_quantity = EXCLUDED.short_term_quantity,
                short_term_value = EXCLUDED.short_term_value,
                short_term_current_value = EXCLUDED.short_term_current_value,
                long_term_price = EXCLUDED.long_term_price,
                long_term_pl = EXCLUDED.long_term_pl,
                short_term_price = EXCLUDED.short_term_price,
                short_term_pl = EXCLUDED.short_term_pl,
                current_value = EXCLUDED.current_value,
                pl = EXCLUDED.pl,
                plp = EXCLUDED.plp,
                xirr = EXCLUDED.xirr,
                cagr = EXCLUDED.cagr,
                asset_class = excluded.asset_class
            """
        )
        session.execute(sql, row.to_dict())
    session.commit()

    if portfolio != "All":
        df = df[df["portfolio"] == portfolio]

    df.sort_values(by="xirr", ascending=False, inplace=True)
    summary = portfolio_summary(client_pan, portfolio)

    return df.to_dict(orient="records"), summary


def portfolio_summary(client_pan: str, portfolio: str):

    sql = text(
        """
        SELECT
            COALESCE(CAST(SUM(holding_value) AS numeric(14,2)),0) AS holding_value,
            COALESCE(CAST(SUM(current_value) AS numeric(14,2)),0) AS current_value
        FROM portfolio
        WHERE client_pan = :client_pan
        AND (:portfolio = 'All' OR portfolio = :portfolio)
    """
    )

    params = {"client_pan": client_pan, "portfolio": portfolio}

    with engine.connect() as connection:
        df = pd.read_sql(sql, connection, params=params)

    summary = df.iloc[0].to_dict()

    holding_value = float(summary["holding_value"])
    current_value = float(summary["current_value"])

    summary.update(
        {
            "pl": 0,
            "plp": 0,
            "xirr": 0,
            "fv_5y": 0,
            "fv_10y": 0,
            "fv_15y": 0,
        }
    )

    if current_value == 0:
        return summary

    # Profit/Loss
    pl = current_value - holding_value
    summary["pl"] = round(pl, 2)
    summary["plp"] = round((pl / holding_value) * 100, 2)

    # XIRR
    xirr = calc_xirr(client_pan, None, None, current_value)
    summary["xirr"] = xirr

    # Future value projections
    r = xirr / 100

    summary["fv_5y"] = round(current_value * ((1 + r) ** 5), 0)
    summary["fv_10y"] = round(current_value * ((1 + r) ** 10), 0)
    summary["fv_15y"] = round(current_value * ((1 + r) ** 15), 0)

    return summary


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

    if instrument is None and folio is None:
        sql = text(
            """
            SELECT
                transaction_date, holding_value
            FROM transactions
            WHERE
                client_pan = :client_pan AND
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
