using System.Text.Json;

namespace EventHubDataReplication.Models
{
    /// <summary>
    /// Represents a sample event data structure from Event Hub
    /// </summary>
    public class EventData
    {
        public string Id { get; set; } = string.Empty;
        public string EventType { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string Data { get; set; } = string.Empty;
        public Dictionary<string, object> Properties { get; set; } = new();
    }

    /// <summary>
    /// SQL Server entity for storing event data
    /// </summary>
    public class SqlEventRecord
    {
        public string Id { get; set; } = string.Empty;
        public string EventType { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string Data { get; set; } = string.Empty;
        public string Properties { get; set; } = string.Empty; // JSON serialized properties
        public DateTime ProcessedAt { get; set; }
    }

    /// <summary>
    /// Cosmos DB document for storing event data
    /// </summary>
    public class CosmosEventDocument
    {
        public string id { get; set; } = string.Empty; // lowercase 'id' required for Cosmos DB
        public string EventType { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string Data { get; set; } = string.Empty;
        public Dictionary<string, object> Properties { get; set; } = new();
        public DateTime ProcessedAt { get; set; }
        public string PartitionKey { get; set; } = string.Empty;
    }

    /// <summary>
    /// Table Storage entity for storing event data
    /// </summary>
    public class TableEventEntity
    {
        public string PartitionKey { get; set; } = string.Empty;
        public string RowKey { get; set; } = string.Empty;
        public string EventType { get; set; } = string.Empty;
        public string Source { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string Data { get; set; } = string.Empty;
        public string Properties { get; set; } = string.Empty; // JSON serialized properties
        public DateTime ProcessedAt { get; set; }
    }
}