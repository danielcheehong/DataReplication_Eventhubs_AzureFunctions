using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using EventHubDataReplication.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace EventHubDataReplication.Functions
{
    public class CosmosDbFunction
    {
        private readonly ILogger<CosmosDbFunction> _logger;

        public CosmosDbFunction(ILogger<CosmosDbFunction> logger)
        {
            _logger = logger;
        }

        [Function("ProcessEventToCosmosDB")]
        public async Task Run([EventHubTrigger("events-cosmos", Connection = "EventHubConnectionString")] Azure.Messaging.EventHubs.EventData[] events)
        {
            var connectionString = Environment.GetEnvironmentVariable("CosmosDBConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogError("Cosmos DB connection string not found in environment variables");
                return;
            }
            foreach (var eventData in events)
            {
                try
                {
                    _logger.LogInformation($"Processing event {eventData.Body} to Cosmos DB");

                    // Deserialize the event data
                    var eventObj = JsonSerializer.Deserialize<Models.EventData>(eventData.Body.ToString());
                    if (eventObj == null)
                    {
                        _logger.LogWarning($"Failed to deserialize event data: {eventData.Body}");
                        continue;
                    }

                    // Create Cosmos DB document
                    var cosmosDocument = new CosmosEventDocument
                    {
                        id = eventObj.Id,
                        EventType = eventObj.EventType,
                        Source = eventObj.Source,
                        Timestamp = eventObj.Timestamp,
                        Data = eventObj.Data,
                        Properties = eventObj.Properties,
                        ProcessedAt = DateTime.UtcNow,
                        PartitionKey = eventObj.EventType // Use EventType as partition key
                    };

                    // Add to Cosmos DB
                    await SaveToCosmosDB(cosmosDocument, connectionString);

                    _logger.LogInformation($"Successfully processed event {cosmosDocument.id} to Cosmos DB");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error processing event: {ex.Message}");
                    throw; // Re-throw to ensure proper error handling by the runtime
                }
            }
        }

        private async Task SaveToCosmosDB(CosmosEventDocument document, string connectionString)
        {
            var cosmosClient = new CosmosClient(connectionString);
            var database = await cosmosClient.CreateDatabaseIfNotExistsAsync("EventDatabase");
            var container = await database.Database.CreateContainerIfNotExistsAsync("EventContainer", "/PartitionKey");
            
            await container.Container.CreateItemAsync(document, new PartitionKey(document.PartitionKey));
        }
    }
}