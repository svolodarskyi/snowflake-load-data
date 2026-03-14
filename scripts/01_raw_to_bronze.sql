v_load_id STRING;
v_load_id := 'LOAD_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISSFF3');

COPY INTO BRONZE.BRONZE_USERS (
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
        $1                          AS UserId,
        $2                          AS FirstName,
        $3                          AS LastName,
        $4                          AS Email,
        $5                          AS Phone,
        $6                          AS Country,
        $7                          AS IsActive,
        $8                          AS CreatedAt,
        $9                          AS UpdatedAt,
        METADATA$FILENAME           AS SourceFilePath,
        METADATA$FILE_ROW_NUMBER    AS SourceFileRowNumber,
        METADATA$FILE_LAST_MODIFIED AS SourceFileLastModified,
        METADATA$START_SCAN_TIME    AS LoadTs,
        :v_load_id                  AS LoadId
    FROM @RAW.STG_RAW/users/
        (FILE_FORMAT => 'RAW.FF_USERS_CSV')
)
ON_ERROR = 'CONTINUE';