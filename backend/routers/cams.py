from fastapi import APIRouter, UploadFile, Form
import requests
import pdfplumber
import re
import pandas as pd
import io
from core.dependencies import engine, NAV_FILE_PATH
from core.dependencies import wealth_transactions
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from typing import Optional

router = APIRouter()


ALLOWED_FILE_TYPES = ["application/pdf"]


@router.post("/upload")
async def get_cams_data(file: UploadFile = Form(...), client_pan: str = Form(...)):
    file_content = await file.read()

    if file.content_type not in ALLOWED_FILE_TYPES:
        return {
            "status": "error",
            "message": f"Unsupported file type. Only PDF files are allowed. but received {file.content_type}",
            "data": [],
        }

    data = camspdf_extraction(
        io.BytesIO(file_content), password=client_pan, client_pan=client_pan
    )

    if not isinstance(data, pd.DataFrame):
        return {
            "status": "error",
            "message": "Failed to parse the PDF file",
            "data": [],
        }

    update_database(data)

    return {
        "status": "success",
        "message": "File uploaded successfully",
        "data": {
            "pan": client_pan,
            "transactions": data.to_dict(orient="records"),
        },
    }


def update_database(data: pd.DataFrame):

    Session = sessionmaker(bind=engine)
    session = Session()

    records = data.to_dict(orient="records")

    with session.begin():
        stmt = insert(wealth_transactions).values(records)

        stmt = stmt.on_conflict_do_update(
            index_elements=[
                "client_pan",
                "folio",
                "instrument",
                "transaction_date",
                "transaction_id",
            ],  # your unique constraint columns
            set_={
                col: stmt.excluded[col]
                for col in [
                    "portfolio",
                    "asset_class",
                    "folio_name",
                    "instrument_name",
                    "transaction_type",
                    "value",
                    "quantity",
                    "price",
                ]
            },
        )

        session.execute(stmt)
    pass


def camspdf_extraction(pdf_path, password=None, client_pan=None):

    url = "https://www.amfiindia.com/spages/NAVOpen.txt"
    password = client_pan.lower() if client_pan else None
    response = requests.get(url)
    if response.status_code == 200:
        amfi_data = response.text.split("\n")
    else:
        amfi_data = None

    final_text = ""
    with pdfplumber.open(pdf_path, password=password) as pdf:
        for i in range(len(pdf.pages)):
            txt = pdf.pages[i].extract_text()
            final_text = final_text + "\n" + txt
        pdf.close()

    folio_pat = re.compile(r"(^Folio No: \d+\s*/\s*\S+)", flags=re.IGNORECASE)
    folio_match = re.compile(r"Folio No: \s*(.*?)\s*(KYC|PAN)", flags=re.IGNORECASE)

    fund_name = re.compile(r".*[Fund].*ISIN.*", flags=re.IGNORECASE)
    # Extracting Transaction data
    trans_details = re.compile(
        r"(^\d{2}-\w{3}-\d{4})(\s.+?\s(?=[\d(]))([\d\(]+[,.]\d+[.\d\)]+)(\s[\d\(\,\.\)]+)(\s[\d\,\.]+)(\s[\d,\.]+)"
    )
    isin_regex = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b", flags=re.IGNORECASE)
    # isin_regex = re.compile(r"\bIN[A-Z0-9]{10}\b", flags=re.IGNORECASE)

    fund_name_regex = re.compile(r"- (.*?) - ISIN", flags=re.IGNORECASE)
    text = ""
    fname = ""
    folio = ""
    folio_new = ""
    isin = ""
    line_itms = []
    for i in final_text.splitlines():
        if folio_match.match(i):
            folio = folio_match.match(i).group(1)

        if fund_name.match(i):
            fname = fund_name.match(i).group(0)
            isin = isin_regex.search(fname).group(0)

        amc, fname, nav = search_isin(isin, amfi_data)
        txt = trans_details.search(i)
        if txt:
            date = txt.group(1)
            description = txt.group(2)
            investment_amount = txt.group(3)
            units = txt.group(4)
            nav = txt.group(5)
            unit_bal = txt.group(6)
            fname = fname
            amc_name = amc

            line_itms.append(
                [
                    folio,
                    isin,
                    fname,
                    amc_name,
                    date,
                    description,
                    investment_amount,
                    units,
                    nav,
                    unit_bal,
                ]
            )

    df = pd.DataFrame(
        line_itms,
        columns=[
            "folio",
            "isin",
            "fund_name",
            "amc_name",
            "date",
            "description",
            "investment_amount",
            "units",
            "nav",
            "unitbalance",
        ],
    )

    df.investment_amount = df.investment_amount.str.replace(",", "")
    df.investment_amount = df.investment_amount.str.replace("(", "-")
    df.investment_amount = df.investment_amount.str.replace(")", "")
    df.investment_amount = df.investment_amount.astype("float")

    df.units = df.units.str.replace(",", "")
    df.units = df.units.str.replace("(", "-")
    df.units = df.units.str.replace(")", "")
    df.units = df.units.astype("float")
    df.units = df.units.round(3)

    df.nav = df.nav.str.replace(",", "")
    df.nav = df.nav.str.replace("(", "-")
    df.nav = df.nav.str.replace(")", "")
    df.nav = df.nav.astype("float")

    df.unitbalance = df.unitbalance.str.replace(",", "")
    df.unitbalance = df.unitbalance.str.replace("(", "-")
    df.unitbalance = df.unitbalance.str.replace(")", "")
    df.unitbalance = df.unitbalance.astype("float")
    df.unitbalance = df.unitbalance.round(3)

    df["client_pan"] = client_pan
    df.folio = df.folio.str.replace("Folio No: ", "")
    df.folio = df.folio.str.replace(" ", "")
    df["isin"] = df["isin"].str.upper()
    df["folio_isin"] = df["folio"] + " (" + df["isin"] + ")"

    df.date = pd.to_datetime(df.date, format="%d-%b-%Y")
    df["description"] = df["units"].apply(lambda x: "buy" if x > 0 else "sell")
    newdf = pd.DataFrame(
        columns=[
            "client_pan",
            "portfolio",
            "asset_class",
            "folio",
            "folio_name",
            "instrument",
            "instrument_name",
            "transaction_date",
            "transaction_type",
            "price",
            "quantity",
            "value",
            "transaction_id",
        ]
    )
    newdf["client_pan"] = df["client_pan"]
    newdf["folio"] = df["folio"]
    newdf["folio_name"] = df["amc_name"]
    newdf["instrument"] = df["isin"]
    newdf["instrument_name"] = df["fund_name"]
    newdf["transaction_date"] = df["date"]
    newdf["transaction_type"] = df["description"]
    newdf["value"] = df["investment_amount"]
    newdf["quantity"] = df["units"].round(3)
    newdf["asset_class"] = "Mutual Fund"
    newdf["portfolio"] = "Mutual Fund"
    newdf["txn_seq"] = (
        newdf.groupby(
            ["client_pan", "folio", "instrument", "transaction_date"]
        ).cumcount()
        + 1
    )
    newdf["transaction_id"] = newdf.apply(
        lambda row: f"{row['transaction_date'].strftime('%Y%m%d')}-{row['txn_seq']}",
        axis=1,
    )
    newdf["price"] = newdf.apply(
        lambda row: row["value"] / row["quantity"] if row["quantity"] != 0 else 0,
        axis=1,
    )
    newdf.drop(columns=["txn_seq"], inplace=True)
    return newdf


def search_isin(isin, amfi_data):

    if amfi_data:
        amc_name = None  # Store AMC name

        for i in range(1, len(amfi_data) - 1):
            if (
                not amfi_data[i].strip()
                and amfi_data[i - 1].strip()
                and amfi_data[i + 1].strip()
            ):
                amc_name = amfi_data[i - 1].strip()

            if isin.upper() in amfi_data[i].upper():
                row = amfi_data[i].split(";")
                fund_name = row[3].strip() if len(row) > 4 else None

                if fund_name:
                    cleaned_text = re.sub(r"\s*\(.*?\)", "", fund_name)
                    cleaned_text = re.sub(
                        r"\b(DIRECT|PLAN|GROWTH|OPTION)\b",
                        "",
                        fund_name,
                        flags=re.IGNORECASE,
                    )
                    cleaned_text = re.sub(r"\s*-\s*", " ", cleaned_text)
                    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
                    fund_name = cleaned_text

                nav = row[4].strip() if len(row) > 4 else None
                nav_date = row[5].strip() if len(row) > 5 else None
                return amc_name, fund_name, nav

    return None, None, None
