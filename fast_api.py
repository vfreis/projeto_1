from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import sqlalchemy as db

app = FastAPI()

# Database setup
DATABASE_URL = "sqlite:///./test.db"
engine = db.create_engine(DATABASE_URL)

# Define data model
class DataItem(BaseModel):
    name: str
    value: float

@app.post("/ingest/")
async def ingest_data(item: DataItem):
    try:
        # Transform data
        transformed_data = {"name": item.name.upper(), "value": item.value * 1.2}
        
        # Load to database
        pd.DataFrame([transformed_data]).to_sql("data_table", con=engine, if_exists="append", index=False)
        
        return {"status": "Data ingested and transformed successfully!"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error: {e}")
