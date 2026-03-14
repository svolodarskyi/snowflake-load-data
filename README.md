## Pipeline Flow
RAW (stage)
   │
   ▼
COPY INTO
   │
   ▼
BRONZE.BRONZE_USERS
   └─ BronzeRowId identity
   │
   ▼
MERGE
   │
   ▼
SILVER.USERS
