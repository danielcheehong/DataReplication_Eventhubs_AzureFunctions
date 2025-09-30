/*
    Script: create-EventReplication-db.sql
    Purpose: Provision local development database for the Event Replication sample.
    Creates:
      - EventReplication database (if not exists)
      - Source table: Client  (simulates upstream operational table that producer polls)
      - Target table: EventData (sink for replicated events in SqlServerFunction)
    Safe to run multiple times (idempotent checks where possible).
*/

IF DB_ID(N'EventReplication') IS NULL
BEGIN
    PRINT 'Creating database EventReplication';
    CREATE DATABASE EventReplication;
END
ELSE
BEGIN
    PRINT 'Database EventReplication already exists';
END
GO

USE EventReplication;
GO

/* Source table polled by producer */
IF OBJECT_ID(N'dbo.Client', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Client
    (
        Id BIGINT IDENTITY(1,1) PRIMARY KEY,
        EventType NVARCHAR(100) NOT NULL,
        Source NVARCHAR(200) NOT NULL,
        [Timestamp] DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        Data NVARCHAR(MAX) NULL,
        Properties NVARCHAR(MAX) NULL,
        CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_Client_Timestamp ON dbo.Client([Timestamp]);
    PRINT 'Created table dbo.Client';
END
ELSE
BEGIN
    PRINT 'Table dbo.Client already exists';
END
GO

/* Target table populated by consumer SqlServerFunction */
IF OBJECT_ID(N'dbo.EventData', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.EventData
    (
        Id BIGINT NOT NULL PRIMARY KEY,
        EventType NVARCHAR(100) NOT NULL,
        Source NVARCHAR(200) NOT NULL,
        [Timestamp] DATETIME2 NOT NULL,
        Data NVARCHAR(MAX) NULL,
        Properties NVARCHAR(MAX) NULL,
        ProcessedAt DATETIME2 NOT NULL,
        InsertedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_EventData_EventType ON dbo.EventData(EventType);
    CREATE INDEX IX_EventData_Timestamp ON dbo.EventData([Timestamp]);
    PRINT 'Created table dbo.EventData';
END
ELSE
BEGIN
    PRINT 'Table dbo.EventData already exists';
END
GO

/* Optional seed rows for Client to trigger producer (only insert if empty) */
IF NOT EXISTS (SELECT 1 FROM dbo.Client)
BEGIN
    INSERT INTO dbo.Client (EventType, Source, Data, Properties)
    VALUES
        (N'CustomerCreated', N'app.core', N'{"customerId":1,"name":"Alice"}', N'{"tier":"gold"}'),
        (N'CustomerUpdated', N'app.core', N'{"customerId":1,"name":"Alice A."}', N'{"tier":"gold"}'),
        (N'OrderPlaced', N'app.orders', N'{"orderId":5001,"amount":123.45}', N'{"currency":"USD"}');
    PRINT 'Seeded initial rows into dbo.Client';
END
ELSE
BEGIN
    PRINT 'dbo.Client already has data; skipping seed.';
END
GO

PRINT 'EventReplication database setup complete.';
