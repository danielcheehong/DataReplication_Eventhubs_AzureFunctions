-- Create database table for SQL Server Function
CREATE DATABASE IF NOT EXISTS EventDatabase;
USE EventDatabase;

CREATE TABLE EventData (
    Id NVARCHAR(50) PRIMARY KEY,
    EventType NVARCHAR(100) NOT NULL,
    Source NVARCHAR(200) NOT NULL,
    Timestamp DATETIME2 NOT NULL,
    Data NVARCHAR(MAX),
    Properties NVARCHAR(MAX), -- JSON formatted properties
    ProcessedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    
    INDEX IX_EventData_EventType (EventType),
    INDEX IX_EventData_Timestamp (Timestamp),
    INDEX IX_EventData_ProcessedAt (ProcessedAt)
);

-- Sample query to view processed events
-- SELECT * FROM EventData ORDER BY ProcessedAt DESC;