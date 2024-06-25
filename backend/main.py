from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from routers import flight_router
from fastapi.responses import RedirectResponse
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


@app.get("/", include_in_schema=False)
def docs_redirect():
    return RedirectResponse(url='/docs')


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
