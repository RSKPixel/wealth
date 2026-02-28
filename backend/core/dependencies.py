from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pathlib import Path
import os
from sqlalchemy import Table, MetaData


# BASE_DIR = Path(__file__).resolve().parent.parent
# DATA_DIR = os.path.join(BASE_DIR, "data")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NAV_FILE_PATH = Path(BASE_DIR) / "data" / "NAVOpen.txt"
url = "https://www.amfiindia.com/spages/NAVAll.txt"

DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username="sysadmin",
    password="Apple@1239",
    host="trialnerror.in",
    port=5432,
    database="wealth",
)

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

metadata = MetaData()
wealth_transactions = Table("wealth_transactions", metadata, autoload_with=engine)
mutualfund_eod = Table("mutualfund_eod", metadata, autoload_with=engine)
