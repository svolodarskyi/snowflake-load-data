USE DATABASE ANALYTICS_DEV;

BEGIN
    LET v_last_row_id NUMBER := 0;

    SELECT COALESCE(MAX(BronzeRowId), 0)
    INTO :v_last_row_id
    FROM SILVER.USERS;

    MERGE INTO SILVER.USERS tgt
    USING (
        SELECT
            b.BronzeRowId,
            TRY_TO_NUMBER(b.UserId) AS UserId,
            b.FirstName,
            b.LastName,
            b.Email,
            b.Phone,
            b.Country,
            IFF(b.IsActive = '1', TRUE, FALSE) AS IsActive,
            TRY_TO_TIMESTAMP_TZ(b.CreatedAt) AS SourceInsertedAt,
            TRY_TO_TIMESTAMP_TZ(b.UpdatedAt) AS SourceUpdatedAt,
            b.SourceFilePath,
            b.SourceFileRowNumber,
            b.SourceFileLastModified,
            b.LoadTs,
            b.LoadId
        FROM BRONZE.USERS b
        WHERE b.BronzeRowId > :v_last_row_id
          AND TRY_TO_NUMBER(b.UserId) IS NOT NULL
          AND b.IsActive IN ('0','1')
          AND TRY_TO_TIMESTAMP_TZ(b.CreatedAt) IS NOT NULL
          AND TRY_TO_TIMESTAMP_TZ(b.UpdatedAt) IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(b.UserId)
            ORDER BY
                TRY_TO_TIMESTAMP_TZ(b.UpdatedAt) DESC,
                b.BronzeRowId DESC
        ) = 1
    ) src
    ON tgt.UserId = src.UserId
    WHEN MATCHED
     AND src.SourceUpdatedAt > tgt.SourceUpdatedAt
    THEN UPDATE SET
        FirstName = src.FirstName,
        LastName = src.LastName,
        Email = src.Email,
        Phone = src.Phone,
        Country = src.Country,
        IsActive = src.IsActive,
        SourceInsertedAt = src.SourceInsertedAt,
        SourceUpdatedAt = src.SourceUpdatedAt,
        SourceFilePath = src.SourceFilePath,
        SourceFileRowNumber = src.SourceFileRowNumber,
        SourceFileLastModified = src.SourceFileLastModified,
        LoadTs = src.LoadTs,
        LoadId = src.LoadId,
        BronzeRowId = src.BronzeRowId
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
        LoadTs,
        LoadId,
        BronzeRowId
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
        src.LoadTs,
        src.LoadId,
        src.BronzeRowId
    );
END;