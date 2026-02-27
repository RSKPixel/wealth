from fastapi import APIRouter

router = APIRouter()


@router.get("/")
def get_mutual_funds():

    return {"message": "Helloworld from mutual fund router"}
