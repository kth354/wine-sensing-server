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
    sensor_id: str
    temperature: float
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

@app.get("/api/sensors/{sensor_id}")
async def get_log(sensor_id:str):
    cursor = db.logs.find({"sensor_id": sensor_id}).sort("timestamp", -1).limit(50)
    logs= await cursor.to_list(length=50)

    if not logs:
        raise HTTPException(status_code=404, detail="로그를 찾을 수 없습니다.")

    chronological_logs = logs[::-1]

    consecutive_24_count = 0
    consecutive_29_count = 0
    final_grade = "A"

    for log in chronological_logs:
        log["_id"] = str(log["_id"])
        temp = log["temperature"]

        if temp >= 29.0:
            consecutive_29_count += 1
        else:
            consecutive_29_count = 0

        if temp >= 24.0:
            consecutive_24_count += 1
        else:
            consecutive_24_count = 0

        if consecutive_29_count >= 3:
            final_grade = "C"
        elif consecutive_24_count >= 3 and final_grade == "A":
            final_grade = "B"

    return {
        "status": "success",
        "sensor_id": sensor_id,
        "count": len(logs),
        "quality_grade": final_grade,
        "data": logs
    }