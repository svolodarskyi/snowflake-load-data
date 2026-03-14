## Pipeline Flow

```mermaid
flowchart TD
    A[RAW Stage] --> B[COPY INTO]
    B --> C[BRONZE.BRONZE_USERS]
    C --> D[MERGE]
    D --> E[SILVER.USERS]
