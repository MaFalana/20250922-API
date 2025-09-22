import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse

router = APIRouter(prefix="/download", tags=["Download"])