CREATE OR REPLACE TABLE ANALYTICS_DEV.BRONZE.USERS (
    BronzeRowId NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    UserId STRING,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    Phone STRING,
    Country STRING,
    IsActive STRING,
    CreatedAt STRING,
    UpdatedAt STRING,
    SourceFilePath STRING,
    SourceFileRowNumber NUMBER,
    SourceFileLastModified TIMESTAMP_NTZ,
    LoadTs TIMESTAMP_NTZ,
    LoadId STRING
);


CREATE OR REPLACE TABLE ANALYTICS_DEV.SILVER.USERS (
    UserId NUMBER,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    Phone STRING,
    Country STRING,
    IsActive BOOLEAN,
    SourceInsertedAt TIMESTAMP_TZ,
    SourceUpdatedAt TIMESTAMP_TZ,
    SourceFilePath STRING,
    SourceFileRowNumber NUMBER,
    SourceFileLastModified TIMESTAMP_NTZ,
    LoadTs TIMESTAMP_NTZ,
    LoadId STRING,
    BronzeRowId NUMBER
);