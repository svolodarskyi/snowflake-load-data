-- ============================================================
-- 1. SET CONTEXT
-- ============================================================
---> set Role Context
USE ROLE ACCOUNTADMIN;
---> set Warehouse Context
USE WAREHOUSE COMPUTE_DEV;
---> set the Database
USE DATABASE ANALYTICS_DEV;
USE SCHEMA SILVER;

-- ============================================================
-- 2. CREATE STORED PROCEDURE
-- ============================================================
CREATE OR REPLACE PROCEDURE ANALYTICS_DEV.SILVER.SP_LOAD_USERS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_last_load_ts      STRING       := '1900-01-01';
    v_rows_affected     NUMBER       := 0;
    v_status            VARCHAR      := 'SUCCESS';
    v_error_message     VARCHAR      := NULL;
    v_start_time        TIMESTAMP_TZ := CURRENT_TIMESTAMP();
    v_result            VARIANT;
BEGIN
    -- --------------------------------------------------------
    -- STEP 1: Get watermark - last processed LoadTs from Silver
    -- --------------------------------------------------------
    SELECT COALESCE(MAX(BronzeLoadTs), '1900-01-01')
    INTO :v_last_load_ts
    FROM ANALYTICS_DEV.SILVER.USERS;

    -- --------------------------------------------------------
    -- STEP 2: Merge Bronze -> Silver
    -- --------------------------------------------------------
    MERGE INTO ANALYTICS_DEV.SILVER.USERS tgt
    USING (
        SELECT
            TRY_TO_NUMBER(b.UserId)  AS UserId,
            b.FirstName,
            b.LastName,
            b.Email,
            b.Phone,
            b.Country,
            b.IsActive,
            b.CreatedAt              AS SourceInsertedAt,
            b.UpdatedAt              AS SourceUpdatedAt,
            b.SourceFilePath,
            b.SourceFileRowNumber,
            b.SourceFileLastModified,
            b.LoadTs                 AS BronzeLoadTs,
            b.LoadId                 AS BronzeLoadId
        FROM ANALYTICS_DEV.BRONZE.USERS b
        WHERE b.LoadTs > :v_last_load_ts
          AND TRY_TO_NUMBER(b.UserId) IS NOT NULL
          AND b.IsActive IN ('0', '1')
          AND b.CreatedAt IS NOT NULL
          AND b.UpdatedAt IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(b.UserId)
            ORDER BY
                b.UpdatedAt DESC,
                b.LoadTs DESC
        ) = 1
    ) src
    ON tgt.UserId = src.UserId
    WHEN MATCHED
     AND src.SourceUpdatedAt > tgt.SourceUpdatedAt
    THEN UPDATE SET
        FirstName              = src.FirstName,
        LastName               = src.LastName,
        Email                  = src.Email,
        Phone                  = src.Phone,
        Country                = src.Country,
        IsActive               = src.IsActive,
        SourceInsertedAt       = src.SourceInsertedAt,
        SourceUpdatedAt        = src.SourceUpdatedAt,
        SourceFilePath         = src.SourceFilePath,
        SourceFileRowNumber    = src.SourceFileRowNumber,
        SourceFileLastModified = src.SourceFileLastModified,
        BronzeLoadTs           = src.BronzeLoadTs,
        BronzeLoadId           = src.BronzeLoadId
    WHEN NOT MATCHED THEN INSERT (
        UserId,
        FirstName,
        LastName,
        Email,
        Phone,
        Country,
        IsActive,
        SourceInsertedAt,
        SourceUpdatedAt,
        SourceFilePath,
        SourceFileRowNumber,
        SourceFileLastModified,
        BronzeLoadTs,
        BronzeLoadId
    )
    VALUES (
        src.UserId,
        src.FirstName,
        src.LastName,
        src.Email,
        src.Phone,
        src.Country,
        src.IsActive,
        src.SourceInsertedAt,
        src.SourceUpdatedAt,
        src.SourceFilePath,
        src.SourceFileRowNumber,
        src.SourceFileLastModified,
        src.BronzeLoadTs,
        src.BronzeLoadId
    );

    -- --------------------------------------------------------
    -- STEP 3: Capture rows affected
    -- --------------------------------------------------------
    v_rows_affected := SQLROWCOUNT;

    -- --------------------------------------------------------
    -- STEP 4: Build result object
    -- --------------------------------------------------------
    SELECT OBJECT_CONSTRUCT(
        'status',          :v_status,
        'start_time',      TO_VARCHAR(:v_start_time),
        'end_time',        TO_VARCHAR(CURRENT_TIMESTAMP()),
        'watermark_used',  :v_last_load_ts,
        'rows_affected',   :v_rows_affected,
        'error',           :v_error_message
    ) INTO :v_result;

    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        v_status        := 'FAILED';
        v_error_message := SQLERRM;

        SELECT OBJECT_CONSTRUCT(
            'status',          :v_status,
            'start_time',      TO_VARCHAR(:v_start_time),
            'end_time',        TO_VARCHAR(CURRENT_TIMESTAMP()),
            'watermark_used',  :v_last_load_ts,
            'rows_affected',   0,
            'error',           :v_error_message
        ) INTO :v_result;

        RETURN v_result;
END;
$$;


CALL ANALYTICS_DEV.SILVER.SP_LOAD_USERS();

SELECT * FROM ANALYTICS_DEV.SILVER.USERS