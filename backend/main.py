from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import cams, data, portfolio
import uvicorn

app = FastAPI()

# Mutual Fund Routes
app.include_router(portfolio.router, prefix="/wealth", tags=["mutualfund"])
app.include_router(cams.router, prefix="/mutualfund/cams", tags=["cams"])
app.include_router(data.router, prefix="/mutualfund/data", tags=["eod"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
