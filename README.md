# Azure Functions Event Hub Data Replication

This project demonstrates how to create Azure Functions that consume events from Azure Event Hubs and replicate the data to different database targets:

- **SQL Server Function**: Saves events to SQL Server database
- **Cosmos DB Function**: Saves events to Azure Cosmos DB 
- **Table Storage Function**: Saves events to Azure Table Storage

## Architecture

Each Azure Function is triggered by events from different Event Hub partitions and processes them independently:

```
Event Hub → Azure Functions → Multiple Databases
├── events-sql     → ProcessEventToSqlServer    → SQL Server
├── events-cosmos  → ProcessEventToCosmosDB    → Cosmos DB  
└── events-table   → ProcessEventToTableStorage → Table Storage
```

## Prerequisites

- .NET 8.0 SDK
- Azure subscription
- Azure Event Hubs namespace with event hubs:
  - `events-sql`
  - `events-cosmos` 
  - `events-table`
- Target databases:
  - SQL Server database
  - Azure Cosmos DB account
  - Azure Storage account

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

Update `local.settings.json` with your connection strings:

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

### Azure Deployment

Set the following application settings in your Azure Function App:
- `EventHubConnectionString`
- `SqlConnectionString`
- `CosmosDBConnectionString` 
- `TableStorageConnectionString`

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

## Database Setup

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

The function will automatically create a table named `EventData`.

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
   ```bash
   func start
   ```

### Deployment

Deploy to Azure using:
```bash
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

1. **Connection String Errors**: Verify all connection strings are correctly formatted
2. **Database Permissions**: Ensure the Function App has appropriate database permissions
3. **Event Hub Access**: Verify the Event Hub connection string has correct permissions
4. **Missing Dependencies**: Run `dotnet restore` to ensure all packages are installed

### Logs

Check Function App logs in Azure Portal or use:
```bash
func azure functionapp logstream <function-app-name>
```