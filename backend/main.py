from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from routers import flight_router
import uvicorn

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
    app.include_router(flight_router.router)

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)