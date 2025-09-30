# Azure Functions Event Hub Data Replication

This project demonstrates a fan‑out replication pipeline: a single Azure Event Hub acts as the source of truth for domain events and three Azure Functions consume the stream in parallel—each persisting the data to a different storage technology (SQL Server, Cosmos DB, and Azure Table Storage).

## Architecture (Single Hub Fan‑Out)

All consumer Functions listen to the same Event Hub `events-source`, but each uses its own consumer group to maintain isolated checkpoints and scaling behavior.

```
                    +------------------+
                    |  SqlToEventHub   |  (Timer producer polls SQL -> emits events)
                    |  (Producer)      |
                    +---------+--------+
                              |
                       Event Hub (events-source)
                              |
        +---------------------+---------------------+
        |                     |                     |
  Consumer Group         Consumer Group        Consumer Group
  TargetSqlReplicator    TargetCosmosReplicator TargetTableReplicator
        |                     |                     |
  ProcessEventToSqlServer  ProcessEventToCosmosDB  ProcessEventToTableStorage
        |                     |                     |
     SQL Server            Cosmos DB            Azure Table Storage
```

### Why Separate Consumer Groups?
Each consumer group has its own set of checkpoints and leases, allowing independent replay, scaling, and failure isolation. This avoids contention and prevents one sink’s lag from blocking others.

## Prerequisites

Required locally / for deployment:
- .NET 8.0 SDK
- Azure subscription
- Azure Event Hubs namespace with ONE hub: `events-source`
- Create three consumer groups (besides `$Default`):
  - `TargetSqlReplicator`
  - `TargetCosmosReplicator`
  - `TargetTableReplicator`
- Target data services:
  - SQL Server (local or Azure SQL)
  - Azure Cosmos DB account (Core (SQL) API)
  - Azure Storage (or Azurite for Table Storage locally)

## Project Structure

```
├── Functions/
│   ├── SqlServerFunction.cs     # SQL Server event processor
│   ├── CosmosDbFunction.cs      # Cosmos DB event processor
│   └── TableStorageFunction.cs  # Table Storage event processor
├── Models/
│   └── EventModels.cs          # Data models for different targets
├── Scripts/
│   ├── setup-sql-server.sql    # SQL Server table setup
│   └── sample-event.json       # Sample event structure
├── host.json                   # Function host configuration
├── local.settings.json         # Local development settings
└── Program.cs                  # Function host startup
```

## Configuration

### Local Development

Update `local.settings.json` with your connection strings (example values / placeholders):

```json
{
  "Values": {
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=<storage_account>;AccountKey=<key>;EndpointSuffix=core.windows.net",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
  "EventHubConnectionString": "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>",
    "SqlConnectionString": "Server=<server>;Database=<db>;User Id=<user>;Password=<password>;",
    "CosmosDBConnectionString": "AccountEndpoint=https://<account>.documents.azure.com:443/;AccountKey=<key>;",
    "TableStorageConnectionString": "DefaultEndpointsProtocol=https;AccountName=<storage_account>;AccountKey=<key>;EndpointSuffix=core.windows.net"
  }
}
```

### Event Hub & Consumer Group Setup

Using Azure CLI (replace `<rg>` and `<namespace>`):

```powershell
az eventhubs eventhub create --name events-source --resource-group <rg> --namespace-name <namespace>
az eventhubs eventhub consumer-group create --eventhub-name events-source --name TargetSqlReplicator --resource-group <rg> --namespace-name <namespace>
az eventhubs eventhub consumer-group create --eventhub-name events-source --name TargetCosmosReplicator --resource-group <rg> --namespace-name <namespace>
az eventhubs eventhub consumer-group create --eventhub-name events-source --name TargetTableReplicator --resource-group <rg> --namespace-name <namespace>
```

Verify:

```powershell
az eventhubs eventhub consumer-group list --eventhub-name events-source --resource-group <rg> --namespace-name <namespace>
```

### Azure Deployment Settings
Configure these Application Settings in the Function App:
- `EventHubConnectionString` (namespace-level recommended)
- `SqlConnectionString`
- `CosmosDBConnectionString`
- `TableStorageConnectionString`

Optionally: `ProducerEventHubName` if you want to override the default `events-source`.

## Event Data Structure

Events should follow this JSON structure:

```json
{
  "id": "unique-event-id",
  "eventType": "EventTypeName", 
  "source": "source-system",
  "timestamp": "2024-09-26T14:30:00Z",
  "data": "event-specific-data",
  "properties": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

## Database / Storage Setup

### SQL Server

Run the script in `Scripts/setup-sql-server.sql` to create the required table:

```sql
CREATE TABLE EventData (
    Id NVARCHAR(50) PRIMARY KEY,
    EventType NVARCHAR(100) NOT NULL,
    Source NVARCHAR(200) NOT NULL,
    Timestamp DATETIME2 NOT NULL,
    Data NVARCHAR(MAX),
    Properties NVARCHAR(MAX),
    ProcessedAt DATETIME2 NOT NULL DEFAULT GETUTCDATE()
);
```

### Cosmos DB

The function will automatically create:
- Database: `EventDatabase`
- Container: `EventContainer` 
- Partition Key: `/PartitionKey`

### Table Storage
Automatically creates a table named `EventData`. Local development can use Azurite:

```powershell
azurite --tableHost 127.0.0.1 --queueHost 127.0.0.1 --blobHost 127.0.0.1
```
Connection string example (Azurite):

```
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNO...==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;
```

## Running the Application

### Local Development

1. Install dependencies:
   ```bash
   dotnet restore
   ```

2. Build the project:
   ```bash
   dotnet build
   ```

3. Start the Function App:
  ```powershell
  func start
  ```

### Deployment

Deploy to Azure:
```powershell
func azure functionapp publish <function-app-name>
```

## Features

- **Error Handling**: Comprehensive logging and error handling with retry policies
- **Scalability**: Each function scales independently based on Event Hub partition load
- **Monitoring**: Application Insights integration for telemetry and monitoring
- **Data Transformation**: Event data is transformed to fit each database schema
- **Automatic Provisioning**: Cosmos DB and Table Storage resources are created automatically

## Monitoring

The functions include comprehensive logging for:
- Event processing success/failure
- Database connection issues
- Data transformation errors
- Performance metrics

Monitor through:
- Azure Application Insights
- Function App logs
- Event Hub metrics

## Testing

Send test events to your Event Hubs using the sample format in `Scripts/sample-event.json`. The functions will process events in real-time and store them in the configured databases.

## Troubleshooting

### Common Issues

1. **Connection String Errors**: Check each required setting exists (local.settings.json not deployed)
2. **Consumer Group Missing**: Ensure all three custom consumer groups exist in `events-source`
3. **Event Hub Parsing Error**: Confirm the connection string starts with `Endpoint=sb://` and is namespace-level
4. **Checkpoint/Offset Issues**: Deleting a consumer group in Azure resets offsets; recreate only if you intend a replay
5. **Missing Dependencies**: Run `dotnet restore` if build fails locally

### Logs

Check Function App logs in Azure Portal or:
```powershell
func azure functionapp logstream <function-app-name>
```

## Constants
Centralized hub name & consumer group names are defined in `Constants/EventHubConstants.cs` to prevent typos and ease refactoring.

## Future Enhancements
- Add dead-letter routing (secondary Event Hub or Storage Queue)
- Structured validation & schema versioning
- Observability: partition lag metrics via EventProcessor logs
- Batch size tuning & backpressure strategies