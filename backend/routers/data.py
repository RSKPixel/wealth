from fastapi import APIRouter, Path
import os
import requests
from core.dependencies import BASE_DIR, url, mutualfund_eod, engine
import pandas as pd
from sqlalchemy import text

router = APIRouter()


@router.get("/eod")
def mf_eod():

    amfi_eod()

    return {"status": "success", "message": "EOD data fetched and stored successfully"}


def amfi_eod():

    sql = text("SELECT distinct(instrument) FROM wealth_transactions")

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
        df.to_csv(nav_amfi_csv, index=False)

    except Exception as e:
        print(f"Error fetching or processing AMFI data: {e}")
