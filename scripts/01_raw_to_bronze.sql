USE DATABASE ANALYTICS_DEV;

COPY INTO ANALYTICS_DEV.BRONZE.USERS (
    UserId,
    FirstName,
    LastName,
    Email,
    Phone,
    Country,
    IsActive,
    CreatedAt,
    UpdatedAt,
    SourceFilePath,
    SourceFileRowNumber,
    SourceFileLastModified,
    LoadTs,
    LoadId
)
FROM (
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$START_SCAN_TIME,
        'LOAD_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISSFF3')
    FROM @analytics_dev.raw.azuredata_stage/users/
        (FILE_FORMAT => 'ANALYTICS_DEV.RAW.FF_USERS_CSV')
)
ON_ERROR = 'CONTINUE';