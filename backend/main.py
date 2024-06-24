from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from routers import flight_router
from fastapi.responses import RedirectResponse
from utils.gcp_model import download_blob
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    download_blob('final-lab-model-bucket', 'models/model.pkl', 'assets/model.pkl')
    app.include_router(flight_router.router)


@app.get("/", include_in_schema=False)
def docs_redirect():
    return RedirectResponse(url='/docs')


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
