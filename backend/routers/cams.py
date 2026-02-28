from fastapi import APIRouter, UploadFile, Form
import requests
import pdfplumber
import re
import pandas as pd
import os
import io

router = APIRouter()

ALLOWED_FILE_TYPES = ["application/pdf"]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# make pan optional
@router.post("/upload")
async def get_cams_data(file: UploadFile = Form(...), pan: str = Form(None)):
    file_content = await file.read()

    file_size = round(len(file_content) / 1024, 2)
    file_name = file.filename
    file_type = file.content_type

    if file_type not in ALLOWED_FILE_TYPES:
        return {
            "status": "error",
            "message": f"Unsupported file type. Only PDF files are allowed. but received {file_type}",
            "data": [],
        }

    parsed_data = parse_cams_data(file_content, pan)

    return {
        "status": "success",
        "message": "File uploaded successfully",
        "data": {
            "pan": pan,
        },
    }


def parse_cams_data(file_content: bytes, pan=None):

    password = str.lower(pan) if pan else None

    url = "https://www.amfiindia.com/spages/NAVOpen.txt"
    response = requests.get(url)
    if response.status_code == 200:
        amfi_data = response.text.split("\n")
    else:
        amfi_data = None

    final_text = ""
    with pdfplumber.open(io.BytesIO(file_content), password=password) as pdf:
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

    df["client_pan"] = pan
    df.folio = df.folio.str.replace("Folio No: ", "")
    df.folio = df.folio.str.replace(" ", "")
    df["isin"] = df["isin"].str.upper()
    df["folio_isin"] = df["folio"] + " (" + df["isin"] + ")"

    df.date = pd.to_datetime(df.date, format="%d-%b-%Y")
    df["description"] = df["units"].apply(lambda x: "IN" if x > 0 else "OUT")
    outputfile = os.path.join(BASE_DIR, "data/output.csv")
    newdf = pd.DataFrame(
        columns=[
            "client_pan",
            "folio",
            "fund_name",
            "amc",
            "assetclass",
            "symbol",
            "name",
            "isin",
            "transaction_date",
            "trade_type",
            "nav",
            "quantity",
            "trade_value",
        ]
    )
    newdf["client_pan"] = df["client_pan"]
    newdf["isin"] = df["isin"]
    newdf["folio"] = df["folio"]
    newdf["fund_name"] = df["fund_name"]
    newdf["amc"] = df["amc_name"]
    newdf["transaction_date"] = df["date"]
    newdf["trade_type"] = df["description"]
    newdf["trade_value"] = df["investment_amount"]
    newdf["units"] = df["units"].round(3)
    newdf["nav"] = df["nav"]

    newdf.to_clipboard(index=False)

    return {"status": "success", "message": "File parsed successfully", "data": {}}


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
