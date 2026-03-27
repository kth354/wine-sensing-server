import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from datetime import datetime,timezone
from dotenv import load_dotenv
load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
client = AsyncIOMotorClient(MONGO_URL)
db = client.sensing_project

class WineSensingLog(BaseModel):
    sensor_mac: str
    temperature: float
    fall_detected: float
    humidity: float
    battery: int

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

@asynccontextmanager
async def lifespan(app: FastAPI):
    await client.admin.command('ping')

    yield

    client.close()

app =FastAPI(lifespan=lifespan)

@app.post("/api/sensors")
async def create_log(log: WineSensingLog):
    new_log = await db.logs.insert_one(log.model_dump())
    return {"status": "success", "id": str(new_log.inserted_id)}

@app.get("/api/sensors/{sensor_mac}")
async def get_log(sensor_mac:str):
    cursor = db.logs.find({"sensor_mac": sensor_mac}).sort("timestamp", -1).limit(50)

    logs= await cursor.to_list(length=50)

    if not logs:
        raise HTTPException(status_code=404, detail="Log not found")

    for log in logs:
        log["_id"] = str(log["_id"])

    return {
        "status": "success",
        "sensor_mac": sensor_mac,
        "count": len(logs),
        "data": logs
    }