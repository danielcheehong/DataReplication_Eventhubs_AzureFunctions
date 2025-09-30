using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using System.Text.Json;
using EventHubDataReplication.Models;

namespace EventHubDataReplication.Functions
{
    public class TableStorageFunction
    {
        private readonly ILogger<TableStorageFunction> _logger;

        public TableStorageFunction(ILogger<TableStorageFunction> logger)
        {
            _logger = logger;
        }

    [Function("ProcessEventToTableStorage")]
    public async Task Run([EventHubTrigger(Constants.EventHubConstants.EventHubName,
                        ConsumerGroup = Constants.EventHubConstants.ConsumerGroupTable,
                        Connection = "EventHubConnectionString")] Azure.Messaging.EventHubs.EventData[] events)
        {
            var connectionString = Environment.GetEnvironmentVariable("TableStorageConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("Table Storage connection string not found in environment variables");
                return;
            }

            foreach (var eventData in events)
            {
                try
                {
                    _logger.LogInformation($"Processing event {eventData.Body} to Table Storage");

                    // Deserialize the event data
                    var eventObj = JsonSerializer.Deserialize<Models.EventData>(eventData.Body.ToString());
                    if (eventObj == null)
                    {
                        _logger.LogWarning($"Failed to deserialize event data: {eventData.Body}");
                        continue;
                    }

                    // Create Table Storage entity
                    var tableEntity = new TableEventEntity
                    {
                        PartitionKey = eventObj.EventType,
                        RowKey = eventObj.Id,
                        EventType = eventObj.EventType,
                        Source = eventObj.Source,
                        Timestamp = eventObj.Timestamp,
                        Data = eventObj.Data,
                        Properties = JsonSerializer.Serialize(eventObj.Properties),
                        ProcessedAt = DateTime.UtcNow
                    };

                    // Save to Table Storage
                    await SaveToTableStorage(tableEntity, connectionString);

                    _logger.LogInformation($"Successfully processed event {tableEntity.RowKey} to Table Storage");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error processing event: {ex.Message}");
                    throw; // Re-throw to ensure proper error handling by the runtime
                }
            }
        }

        private async Task SaveToTableStorage(TableEventEntity entity, string connectionString)
        {
            var tableClient = new TableClient(connectionString, "EventData");
            
            // Ensure table exists
            await tableClient.CreateIfNotExistsAsync();

            // Convert to TableEntity
            var tableEntity = new TableEntity(entity.PartitionKey, entity.RowKey)
            {
                ["EventType"] = entity.EventType,
                ["Source"] = entity.Source,
                ["Timestamp"] = entity.Timestamp,
                ["Data"] = entity.Data,
                ["Properties"] = entity.Properties,
                ["ProcessedAt"] = entity.ProcessedAt
            };

            // Upsert the entity
            await tableClient.UpsertEntityAsync(tableEntity);
        }
    }
}