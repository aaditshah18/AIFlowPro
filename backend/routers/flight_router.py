from fastapi import APIRouter, status, HTTPException
from fastapi.responses import JSONResponse
from schemas.flight_request import FlightData
from schemas.prediction_response import DelayedResponse
import random

router = APIRouter(prefix='/flight', tags=['Flight prediction'])


@router.post(
    "/prediction", status_code=status.HTTP_200_OK, response_model=DelayedResponse
)
def get_flight_delays(request: FlightData):

    delayed = random.choice([True, False])
    return DelayedResponse(success=True, delayed=delayed)
