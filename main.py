import os
from contextlib import asynccontextmanager
from re import match

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
        search: Optional[str] = None,
        category: Optional[str] = None
):
    skip_count = (page -1) * limit
    query = {}

    if search:
        query = {
            "$or":[
                {"WINE_NM" : {"$regex": search, "$options":"i"}},
                {"WINE_NM_KR" : {"$regex": search, "$options":"i"}},
                {"WINE_CTGRY" : {"$regex": search, "$options":"i"}},
                {"WINE_AREA_NM" : {"$regex": search, "$options":"i"}}
            ]
        }

    if category:
        query["WINE_CTGRY"] = category

    print(f" 현재 DB 검색 조건: {query}")

    cursor = db.wine_info.find(query).sort("view_count", - 1).skip(skip_count).limit(limit)
    print(f"내림차순 적용")

    cursor = cursor.skip(skip_count).limit(limit)

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

@app.get("/api/wines/ranking")
async def get_wines_ranking():
    cursor = db.wine_info.find().sort("view_count",-1).limit(10)
    wines = await cursor.to_list(length=10)

    top_wines = []
    for wine in wines:
        wine["_id"] = str(wine["_id"])
        wine_nm = wine.get("WINE_NM", "")
        safe_name = urllib.parse.quote(wine_nm)
        wine["image_url"] = f"{S3_BASE_URL}/{safe_name}.png"
        top_wines.append(wine)


    return {"status": "success",
            "returned_count": len(top_wines),
            "data": top_wines
            }

class ViewLog(BaseModel):
    user_id: str
    wine_nm: str
    category: str

@app.post("/api/wines/user_view")
async def save_view_log(log: ViewLog):
    log_data = log.model_dump()

    log_data["viewed_at"] = datetime.now(timezone.utc)

    result = await db.user_view.insert_one(log_data)

    await db.wine_info.update_one(
        {"WINE_NM": log.wine_nm},
        {"$inc": {"view_count": 1}}
    )
    return{
        "status": "success",
        "log_id": str(result.inserted_id)
    }

@app.get("/api/wines/recommend/{user_id}")
async def get_recommend(user_id: str):
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$group": {"_id": "$category", "view_count": {"$sum": 1}}},
        {"$sort": {"view_count": -1}},
        {"$limit": 10},
    ]

    cursor =db.user_view.aggregate(pipeline)
    top_category_result= await cursor.to_list(length=10)

    if not top_category_result:
        return{
            "status": "success",
            "message": "아직 와인 추천이 어려워요",
            "recommendations":[]
        }

    best_category = top_category_result[0]["_id"]

    recommended_cursor = db.wine_info.find(
        {"WINE_CTGRY": best_category}
    ).sort("view_count", -1).limit(5)

    final_wines = await recommended_cursor.to_list(length=5)

    for wine in final_wines:
        wine["_id"] = str(wine["_id"])
        wine_nm = wine.get("WINE_NM", "")
        safe_name = urllib.parse.quote(wine_nm)
        wine["image_url"] = f"{S3_BASE_URL}/{safe_name}.png"

    return{
        "status": "success",
        "user_id": user_id,
        "favorite_category": best_category,
        "recommendations": final_wines
    }

