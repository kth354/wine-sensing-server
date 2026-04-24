import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from datetime import datetime,timezone
from dotenv import load_dotenv
from typing import Optional, List
import urllib.parse
load_dotenv()

S3_BASE_URL = os.getenv("S3_BASE_URL")
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
async def create_log(logs: List[WineSensingLog]):
    log_dicts = [log.model_dump() for log in logs]

    if log_dicts:
        result = await db.logs.insert_many(log_dicts)
        return{
            "status": "success",
            "inserted_count":len(result.inserted_ids),
            "ids": [str(id) for id in result.inserted_ids]
        }
    return {"status": "error", "message":"데이터가 없습니다"}

@app.get("/api/sensors/{sensor_id}")
async def get_log(sensor_id:str):
    cursor = db.logs.find({"sensor_id": sensor_id}).sort("timestamp", 1)
    #과거부터 현재까지 대충 만 개 정도까지 한번에 긁어오기
    logs= await cursor.to_list(length=10000)

    if not logs:
        raise HTTPException(status_code=404, detail="로그를 찾을 수 없습니다.")

    consecutive_24_count = 0
    consecutive_29_count = 0
    final_grade = "A"

    for log in logs:
        log["_id"] = str(log["_id"])
        temp = log["temperature"]
        #유통 등급매기기 C등급 조건
        if temp >= 29.0:
            consecutive_29_count += 1
        else:
            consecutive_29_count = 0
        #b등급 조건
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

@app.get("/api/wines")
async def get_wines(
        page: int =1,
        limit: int = 10,
        search: Optional[str] = None
):
    skip_count = (page -1) * limit
    query = {}

    if search:
        query = {
            "$or":[
                {"WINE_NM" : {"$regex": search, "$options":"i"}},
                {"WINE_CTGRY" : {"$regex": search, "$options":"i"}},
                { "WINE_AREA_NM" : {"$regex": search, "$options":"i"}}
            ]
        }

    print(f" 현재 DB 검색 조건: {query}")

    cursor = db.wine_info.find(query).skip(skip_count).limit(limit)
    wines = await cursor.to_list(length=limit)

    for wine in wines:
        wine["_id"] = str(wine["_id"])
        wine_nm = wine.get("WINE_NM", "")
        safe_name = urllib.parse.quote(wine_nm)
        wine["image_url"] = f"{S3_BASE_URL}/{safe_name}.png"

    return{
        "status": "success",
        "page": page,
        "limit": limit,
        "returned_count": len(wines),
        "data": wines
    }