
USE DATABASE ANALYTICS_DEV;

BEGIN
    LET v_last_load_ts STRING := '0';

    SELECT COALESCE(MAX(BronzeLoadTs), '1900-01-01')
    INTO :v_last_load_ts
    FROM SILVER.USERS;

    MERGE INTO SILVER.USERS tgt
    USING (
        SELECT
            TRY_TO_NUMBER(b.UserId) AS UserId,
            b.FirstName,
            b.LastName,
            b.Email,
            b.Phone,
            b.Country,
            b.IsActive,
            b.CreatedAt AS SourceInsertedAt,
            b.UpdatedAt AS SourceUpdatedAt,
            b.SourceFilePath,
            b.SourceFileRowNumber,
            b.SourceFileLastModified,
            b.LoadTs AS BronzeLoadTs,
            b.LoadId AS BronzeLoadId
        FROM BRONZE.USERS b
        WHERE b.LoadTs > :v_last_load_ts
          AND TRY_TO_NUMBER(b.UserId) IS NOT NULL
          AND b.IsActive IN ('0','1')
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
        BronzeLoadTs = src.BronzeLoadTs,
        BronzeLoadId = src.BronzeLoadId
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
END;

--SELECT * FROM analytics_dev.SILVER.USERS