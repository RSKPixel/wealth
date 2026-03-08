from fastapi import APIRouter, Form
import os
import requests
from core.dependencies import BASE_DIR, url, eod, engine
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/historical")
def mf_historical(range: str = Form("recent")):

    amfi_historical(range=range)

    return {
        "status": "success",
        "message": "Historical data fetched and stored successfully",
    }


def amfi_historical(range: str = "recent"):

    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    sql = text(
        """
        SELECT
            MIN(transaction_date) AS from_date,
            eod.scheme_code as scheme_code,
            transactions.instrument AS instrument,
            transactions.instrument_name AS instrument_name
        FROM
            transactions
        INNER JOIN
            eod
        ON
            transactions.instrument=eod.instrument
        GROUP BY
            transactions.instrument, eod.scheme_code, transactions.instrument_name
        """
    )

    with engine.connect() as connection:
        result = connection.execute(sql)
        df = pd.DataFrame(
            result.fetchall(),
            columns=["from_date", "scheme_code", "instrument", "instrument_name"],
        )

    for index, row in df.iterrows():
        to_date = datetime.now().date()
        scheme_code = row["scheme_code"]
        if range == "max":
            from_date = row["from_date"]
            date_ranges = generate_ranges(from_date, to_date, years=4)
        else:
            from_date = to_date - timedelta(days=60)
            date_ranges = [(from_date, to_date)]

        for from_date, to_date in date_ranges:
            to_date = to_date.strftime("%Y-%m-%d")
            from_date = from_date.strftime("%Y-%m-%d")

            print(
                f"Fetching historical data for {scheme_code} from {from_date} to {to_date}"
            )

            url = f"https://www.amfiindia.com/api/nav-history?query_type=historical_period&from_date={from_date}&to_date={to_date}&sd_id={scheme_code}"

            try:
                session = requests.Session()
                response = session.get(url)
                response.raise_for_status()
                data = response.json()

                mf = pd.DataFrame(data["data"]["nav_groups"][0]["historical_records"])
                mf = mf[["date", "nav"]]

                mf["date"] = pd.to_datetime(mf["date"])
                mf["month_year"] = mf["date"].dt.strftime("%m%Y")
                mf["nav"] = pd.to_numeric(mf["nav"], errors="coerce")
                mf["instrument"] = row["instrument"]
                mf["instrument_name"] = row["instrument_name"]

                mf = mf.set_index("date").resample("ME").last().reset_index()
                mf.rename(columns={"nav": "close_price"}, inplace=True)

                Session = sessionmaker(bind=engine)
                session = Session()
                for _, row in mf.iterrows():
                    sql = text(
                        """
                        INSERT INTO
                            historical
                            (month_year, close_price, instrument, instrument_name, date)
                        VALUES (:month_year, :close_price, :instrument, :instrument_name, :date)
                        ON CONFLICT (month_year, instrument) DO UPDATE SET
                            date = EXCLUDED.date,
                            close_price = EXCLUDED.close_price
                        """
                    )
                    session.execute(sql, row.to_dict())
                session.commit()
                session.close()

            except Exception as e:
                print(f"Error fetching historical data for {row['instrument']}: {e}")
                continue


@router.get("/eod")
def mf_eod():

    amfi_eod()

    return {"status": "success", "message": "EOD data fetched and stored successfully"}


def amfi_eod():

    sql = text(
        "SELECT distinct(instrument) FROM transactions WHERE portfolio = 'Mutual Fund'"
    )

    with engine.connect() as connection:
        result = connection.execute(sql)
        instruments = [row[0] for row in result]

    try:
        session = requests.Session()
        response = session.get(url)
        response.raise_for_status()
        data = response.text.splitlines()

        mf_asset_class = pd.read_csv(os.path.join(BASE_DIR, "data", "mfac.csv"))

        nav_amfi_csv = os.path.join(BASE_DIR, "data", "mf_nav_amfi.csv")
        nav_amfi_txt = os.path.join(BASE_DIR, "data", "mf_nav_amfi.txt")
        amc_code_df = pd.read_csv(os.path.join(BASE_DIR, "data", "amfi_amc.csv"))

        with open(nav_amfi_txt, "wb") as file:
            file.write(response.content)

        with open(nav_amfi_txt, "r", encoding="utf-8", errors="replace") as infile:
            lines = infile.readlines()

        amc_name = None
        amc_code = None

        with open(nav_amfi_csv, "w", encoding="utf-8") as outfile:
            for i in range(1, len(lines) - 1):
                current = lines[i].strip()
                prev = lines[i - 1].strip()
                next = lines[i + 1].strip()

                # Check if current line is AMC name (surrounded by blank lines)
                if current and not prev and not next:
                    amc_name = current
                    if amc_name in amc_code_df["amc_name"].values:
                        amc_code = amc_code_df[amc_code_df["amc_name"] == amc_name][
                            "amc_code"
                        ].values[0]
                    else:
                        amc_code = None
                    continue

                # Valid data line: 5 semicolons and an AMC name identified
                if lines[i].count(";") == 5 and amc_name:
                    outfile.write(lines[i].strip() + f";{amc_name};{amc_code}\n")

        # Read the cleaned CSV file

        df = pd.read_csv(nav_amfi_csv, sep=";")
        df.columns = [
            "scheme_code",
            "isin_1",
            "isin_2",
            "scheme_name",
            "nav",
            "nav_date",
            "amc_name",
            "amc_code",
        ]

        df = df[
            [
                "nav_date",
                "scheme_code",
                "scheme_name",
                "amc_name",
                "amc_code",
                "isin_1",
                "nav",
                "isin_2",
            ]
        ]

        df["nav_date"] = pd.to_datetime(df["nav_date"], format="%d-%b-%Y").dt.strftime(
            "%Y-%m-%d"
        )

        df["nav"] = df["nav"].replace("N.A.", 0)
        df["amc_code"] = df["amc_code"].astype("Int64")
        df["scheme_code"] = df["scheme_code"].astype(str)

        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.rename(
            columns={
                "nav_date": "date",
                "scheme_code": "scheme_code",
                "scheme_name": "scheme_name",
                "amc_name": "amc_name",
                "amc_code": "amc_code",
                "isin_1": "isin_1",
                "isin_2": "isin_2",
                "nav": "nav",
            }
        )
        df["scheme_type"] = df["isin_1"].apply(
            lambda x: (
                mf_asset_class.loc[mf_asset_class["isin"] == x, "scheme_type"].values[0]
                if x in mf_asset_class["isin"].values
                else None
            )
        )

        # if scheme_type is Debt, then asset_class is Debt else it is Equity
        df["asset_class"] = df["scheme_type"].apply(
            lambda x: "Debt" if x == "Debt" else "Equity"
        )

        # if scheme_name contains Gold then asset_class is Gold else it is Equity
        df["asset_class"] = df.apply(
            lambda x: "Gold" if "Gold" in x["scheme_name"] else x["asset_class"], axis=1
        )

        df = df[
            [
                "date",
                "scheme_code",
                "scheme_name",
                "amc_code",
                "amc_name",
                "isin_1",
                "isin_2",
                "nav",
                "asset_class",
                "scheme_type",
            ]
        ]

        df = df[df["isin_1"].isin(instruments)]

        # save to table
        Session = sessionmaker(bind=engine)
        session = Session()

        for _, row in df.iterrows():
            insert_query = text(
                """
                INSERT INTO eod (
                    date, scheme_code, instrument_name, amc_code, amc_name, instrument, current_price, asset_class, scheme_type
                ) VALUES (
                    :date, :scheme_code, :scheme_name, :amc_code, :amc_name, :isin_1, :nav, :asset_class, :scheme_type
                )
                ON CONFLICT (instrument) DO UPDATE SET
                    date = EXCLUDED.date,
                    current_price = EXCLUDED.current_price
                """
            )
            session.execute(insert_query, row.to_dict())

        session.commit()
        session.close()

        df.to_csv(nav_amfi_csv, index=False)

    except Exception as e:
        print(f"Error fetching or processing AMFI data: {e}")


def generate_ranges(from_date, to_date, years=5):
    ranges = []
    current_from = from_date

    while current_from < to_date:
        current_to = min(current_from + timedelta(days=365 * years), to_date)

        ranges.append((current_from, current_to))

        current_from = current_to + timedelta(days=1)

    return ranges
