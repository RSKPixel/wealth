from fastapi import APIRouter, UploadFile, Form

router = APIRouter()

ALLOWED_FILE_TYPES = ["application/pdf"]


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

    parsed_data = parse_cams_data(file_content)

    return {
        "status": "success",
        "message": "File uploaded successfully",
        "data": {
            "pan": pan,
        },
    }


def parse_cams_data(file_content):

    return {"status": "success", "message": "File parsed successfully", "data": {}}
