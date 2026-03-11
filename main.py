from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import asyncpg
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

pool = None


class PropertyUpdate(BaseModel):
    property_id: int
    price: float
    status: Optional[str] = None


@app.on_event("startup")
async def startup():

    global pool

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10
    )


@app.post("/update-properties")
async def update_properties(data: List[PropertyUpdate]):

    async with pool.acquire() as conn:

        await conn.executemany(
            """
            INSERT INTO properties_clean(property_id, price, status)
            VALUES($1,$2,$3)
            ON CONFLICT (property_id)
            DO UPDATE SET
                price = EXCLUDED.price,
                status = COALESCE(EXCLUDED.status, properties_clean.status)
            """,
            [(p.property_id, p.price, p.status) for p in data]
        )

    return {"updated": len(data)}