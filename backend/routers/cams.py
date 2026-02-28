from fastapi import APIRouter, UploadFile, Form
import requests
import pdfplumber
import re
import pandas as pd
import os
import io
from pathlib import Path

router = APIRouter()

ALLOWED_FILE_TYPES = ["application/pdf"]
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NAV_FILE_PATH = Path(BASE_DIR) / "data" / "NAVOpen.txt"


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


def _clean_numeric_series(series: pd.Series, round_decimals: int = None) -> pd.Series:
    """Strip formatting and convert to float."""
    s = (
        series.str.replace(",", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
        .astype(float)
    )
    return s.round(round_decimals) if round_decimals is not None else s


def _fetch_amfi_data() -> list[str] | None:
    url = "https://www.amfiindia.com/spages/NAVOpen.txt"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        nav_text = response.text

        # Save fresh copy to local archive
        NAV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        NAV_FILE_PATH.write_text(nav_text, encoding="utf-8")

        return nav_text.split("\n")

    except requests.RequestException as e:
        print(f"[AMFI] Failed to fetch live data: {e}. Falling back to archived copy.")

        # Fall back to local archived copy
        if NAV_FILE_PATH.exists():
            print(f"[AMFI] Loading archived NAV data from {NAV_FILE_PATH}")
            return NAV_FILE_PATH.read_text(encoding="utf-8").split("\n")

        print("[AMFI] No archived copy found. ISIN lookup will be unavailable.")
        return None


def _extract_pdf_text(file_content: bytes, password: str | None) -> str:
    with pdfplumber.open(io.BytesIO(file_content), password=password) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def search_isin(isin: str, amfi_data: list[str] | None):
    if not amfi_data:
        return None, None, None

    amc_name = None
    for i in range(1, len(amfi_data) - 1):
        # Detect AMC header lines (surrounded by blank lines)
        if (
            not amfi_data[i].strip()
            and amfi_data[i - 1].strip()
            and amfi_data[i + 1].strip()
        ):
            amc_name = amfi_data[i - 1].strip()

        if isin.upper() not in amfi_data[i].upper():
            continue

        row = amfi_data[i].split(";")
        if len(row) <= 4:
            return amc_name, None, None

        fund_name = row[3].strip()
        nav = row[4].strip()

        # Clean fund name
        fund_name = re.sub(r"\s*\(.*?\)", "", fund_name)
        fund_name = re.sub(
            r"\b(DIRECT|PLAN|GROWTH|OPTION)\b", "", fund_name, flags=re.IGNORECASE
        )
        fund_name = re.sub(r"\s*-\s*", " ", fund_name)
        fund_name = re.sub(r"\s+", " ", fund_name).strip()

        return amc_name, fund_name, nav

    return None, None, None


def parse_cams_data(file_content: bytes, pan: str | None = None):
    password = pan.lower() if pan else None
    amfi_data = _fetch_amfi_data()
    final_text = _extract_pdf_text(file_content, password)

    folio_match = re.compile(r"Folio No:\s*(.*?)\s*(KYC|PAN)", re.IGNORECASE)
    fund_name_re = re.compile(r".*Fund.*ISIN.*", re.IGNORECASE)
    trans_re = re.compile(
        r"(^\d{2}-\w{3}-\d{4})"
        r"(\s.+?\s(?=[\d(]))"
        r"([\d\(]+[,.]\d+[.\d\)]+)"
        r"(\s[\d\(\,\.\)]+)"
        r"(\s[\d\,\.]+)"
        r"(\s[\d,\.]+)"
    )
    isin_re = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b", re.IGNORECASE)

    records = []
    folio = isin = ""
    amc = fname = ""

    for line in final_text.splitlines():
        if m := folio_match.match(line):
            folio = m.group(1)

        if fund_name_re.match(line):
            raw = line.strip()
            isin_match = isin_re.search(raw)
            if isin_match:
                isin = isin_match.group(0)
                amc, fname, _ = search_isin(isin, amfi_data)

                # Fallback: extract fund name directly from the PDF line
                if not fname:
                    fn_match = re.search(r"- (.*?) - ISIN", raw, re.IGNORECASE)
                    fname = fn_match.group(1).strip() if fn_match else raw

        if m := trans_re.search(line):
            # Skip orphan transactions before any fund header is found
            if not fname:
                continue
            records.append(
                {
                    "folio": folio,
                    "isin": isin.upper(),
                    "fund_name": fname,
                    "amc_name": amc,
                    "date": m.group(1),
                    "investment_amount": m.group(3),
                    "units": m.group(4),
                    "nav": m.group(5),
                    "unitbalance": m.group(6),
                }
            )

    df = pd.DataFrame(records)
    if df.empty:
        return {"status": "success", "message": "No transactions found", "data": {}}

    # Clean numeric columns
    df["investment_amount"] = _clean_numeric_series(df["investment_amount"])
    df["units"] = _clean_numeric_series(df["units"], round_decimals=3)
    df["nav"] = _clean_numeric_series(df["nav"])
    df["unitbalance"] = _clean_numeric_series(df["unitbalance"], round_decimals=3)

    # Date and derived columns
    df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y")
    df["trade_type"] = df["units"].apply(lambda x: "IN" if x > 0 else "OUT")
    df["client_pan"] = pan

    # Clean folio - remove "Folio No:" prefix and ALL whitespace including middle
    df["folio"] = (
        df["folio"]
        .str.replace(r"Folio No:\s*", "", regex=True)
        .str.replace(r"\s+", "", regex=True)
    )

    df["folio_isin"] = df["folio"] + " (" + df["isin"] + ")"

    # Build final output DataFrame
    output = df.rename(
        columns={
            "amc_name": "amc",
            "investment_amount": "trade_value",
        }
    )[
        [
            "client_pan",
            "isin",
            "folio",
            "fund_name",
            "amc",
            "date",
            "trade_type",
            "nav",
            "units",
            "trade_value",
        ]
    ]

    output.to_clipboard(index=False)
    return {"status": "success", "message": "File parsed successfully", "data": {}}
