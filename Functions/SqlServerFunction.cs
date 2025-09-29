using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Text.Json;
using EventHubDataReplication.Models;

namespace EventHubDataReplication.Functions
{
    public class SqlServerFunction
    {
        private readonly ILogger<SqlServerFunction> _logger;

        public SqlServerFunction(ILogger<SqlServerFunction> logger)
        {
            _logger = logger;
        }

        [Function("ProcessEventToSqlServer")]
        public async Task Run([EventHubTrigger("events-sql",
                            ConsumerGroup = "TargetReplicator1",
                            Connection = "EventHubConnectionString")
                            ] Azure.Messaging.EventHubs.EventData[] events)
        {
            var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("SQL connection string not found in environment variables");
                return;
            }

            foreach (var eventData in events)
            {
                try
                {
                    _logger.LogInformation($"Processing event {eventData.Body} to SQL Server");

                    // Deserialize the event data
                    var eventObj = JsonSerializer.Deserialize<Models.EventData>(eventData.Body.ToString());
                    if (eventObj == null)
                    {
                        _logger.LogWarning($"Failed to deserialize event data: {eventData.Body}");
                        continue;
                    }

                    // Create SQL record
                    var sqlRecord = new SqlEventRecord
                    {
                        Id = eventObj.Id,
                        EventType = eventObj.EventType,
                        Source = eventObj.Source,
                        Timestamp = eventObj.Timestamp,
                        Data = eventObj.Data,
                        Properties = JsonSerializer.Serialize(eventObj.Properties),
                        ProcessedAt = DateTime.UtcNow
                    };

                    // Save to SQL Server
                    await SaveToSqlServer(sqlRecord, connectionString);

                    _logger.LogInformation($"Successfully processed event {sqlRecord.Id} to SQL Server");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error processing event: {ex.Message}");
                    throw; // Re-throw to ensure proper error handling by the runtime
                }
            }
        }

        private async Task SaveToSqlServer(SqlEventRecord record, string connectionString)
        {
            const string insertQuery = @"
                INSERT INTO EventData (Id, EventType, Source, Timestamp, Data, Properties, ProcessedAt)
                VALUES (@Id, @EventType, @Source, @Timestamp, @Data, @Properties, @ProcessedAt)";

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var command = new SqlCommand(insertQuery, connection);
            command.Parameters.AddWithValue("@Id", record.Id);
            command.Parameters.AddWithValue("@EventType", record.EventType);
            command.Parameters.AddWithValue("@Source", record.Source);
            command.Parameters.AddWithValue("@Timestamp", record.Timestamp);
            command.Parameters.AddWithValue("@Data", record.Data);
            command.Parameters.AddWithValue("@Properties", record.Properties);
            command.Parameters.AddWithValue("@ProcessedAt", record.ProcessedAt);

            await command.ExecuteNonQueryAsync();
        }
    }
}