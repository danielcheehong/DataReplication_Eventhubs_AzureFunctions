using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace EventHubDataReplication.Functions;

/// <summary>
/// Periodically polls a SQL Server table for new rows and publishes them to an Event Hub.
/// Implements exponential backoff with full jitter when no new data is found to reduce load.
/// State tracking (last processed Id / timestamp) is kept in-memory for simplicity; in production you would persist it (Table Storage, SQL watermark table, Blob, etc.).
/// </summary>
public class EHClientProducer
{
	private readonly ILogger<EHClientProducer> _logger;
	private readonly EventHubProducerClient _producerClient;

	// In-memory watermark (demo). Replace with durable store for real scenarios.
	private static long _lastId = 0;

	// Random for jitter
	private static readonly Random _rng = new();

	// Backoff state
	private static int _consecutiveEmptyPolls = 0;

	public EHClientProducer(ILogger<EHClientProducer> logger)
	{
		_logger = logger;

		// Build EventHubProducerClient manually because we publish (output binding is for triggers mainly)
		var eventHubConnection = Environment.GetEnvironmentVariable("EventHubConnectionString");
		var eventHubName = Environment.GetEnvironmentVariable("ProducerEventHubName") ?? "events-sql"; // default reuse

		if (string.IsNullOrWhiteSpace(eventHubConnection))
		{
			throw new InvalidOperationException("EventHubConnectionString app setting is missing.");
		}

		_producerClient = new EventHubProducerClient(eventHubConnection, eventHubName);
	}

	/// <summary>
	/// Timer trigger fires on a fixed short cadence; internal jittered backoff controls actual polling frequency.
	/// Schedule: every 15 seconds. Adjust via CRON if desired.
	/// </summary>
	/// <remarks>
	/// The jittered delay is applied INSIDE the function to avoid editing host.json for dynamic behavior.
	/// </remarks>
	[Function("SqlToEventHubProducer")]    
	public async Task RunAsync([TimerTrigger("*/15 * * * * *")] TimerInfo timerInfo)
	{
		var sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
		var sourceTable = Environment.GetEnvironmentVariable("ProducerSourceTable") ?? "Client"; // user specified table name

		if (string.IsNullOrWhiteSpace(sqlConnectionString))
		{
			_logger.LogError("SqlConnectionString app setting is missing.");
			return;
		}

		// Compute jittered backoff delay if needed BEFORE doing work (skip if we have not had empty polls)
		if (_consecutiveEmptyPolls > 0)
		{
			var delay = ComputeJitterDelay();
			_logger.LogDebug("Backoff active: consecutiveEmptyPolls={count}, delaying {delayMs}ms", _consecutiveEmptyPolls, delay.TotalMilliseconds);
			await Task.Delay(delay);
		}

		try
		{
			var rows = await FetchNewRows(sqlConnectionString, sourceTable, _lastId, batchSize: GetIntEnv("ProducerBatchSize", 100));
            
			if (rows.Count == 0)
            {
                _consecutiveEmptyPolls++;
                _logger.LogInformation("No new rows found (lastId={lastId}). EmptyPolls={emptyPolls}", _lastId, _consecutiveEmptyPolls);
                return;
            }

			// Reset backoff counters on success
			_consecutiveEmptyPolls = 0;

			// Publish in a single batch; could chunk if large
			EventDataBatch currentBatch = await _producerClient.CreateBatchAsync();
			foreach (var row in rows)
			{
				var json = JsonSerializer.Serialize(row);
				var evt = new EventData(new BinaryData(json));
				if (!currentBatch.TryAdd(evt))
				{
					// send current and start a new batch
					await _producerClient.SendAsync(currentBatch);
					currentBatch = await _producerClient.CreateBatchAsync();
					if (!currentBatch.TryAdd(evt))
					{
						_logger.LogError("Single event too large to fit into empty batch; skipping Id={Id}", row.Id);
						continue;
					}
				}
			}
			if (currentBatch.Count > 0)
			{
				await _producerClient.SendAsync(currentBatch);
			}

			// Advance watermark to max id processed
			_lastId = Math.Max(_lastId, rows[^1].Id);
			_logger.LogInformation("Published {count} rows to Event Hub. New lastId={lastId}", rows.Count, _lastId);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error during polling or publishing");
			// Optional: implement retry or partial failure handling
		}
	}

	private static int GetIntEnv(string name, int defaultValue)
		=> int.TryParse(Environment.GetEnvironmentVariable(name), out var v) && v > 0 ? v : defaultValue;

	/// <summary>
	/// Full jitter exponential backoff: base * 2^(k) with cap, then random between 0 and computed.
	/// Env overrides: ProducerBackoffBaseMs (default 500), ProducerBackoffMaxMs (default 15000)
	/// </summary>
	private static TimeSpan ComputeJitterDelay()
	{
		int baseMs = GetIntEnv("ProducerBackoffBaseMs", 500);
		int maxMs = GetIntEnv("ProducerBackoffMaxMs", 15000);
		// k = consecutiveEmptyPolls - 1 (first empty poll we don't delay because it's computed next run)
		int k = Math.Max(0, _consecutiveEmptyPolls - 1);
		double exp = Math.Min(baseMs * Math.Pow(2, k), maxMs);
		int delay = _rng.Next(0, (int)exp + 1);
		return TimeSpan.FromMilliseconds(delay);
	}

	private static async Task<List<DbRow>> FetchNewRows(string connectionString, string table, long lastId, int batchSize)
	{
		var results = new List<DbRow>();
		var query = $"SELECT TOP (@BatchSize) Id, EventType, Source, Timestamp, Data, Properties FROM {table} WITH (READPAST) WHERE Id > @LastId ORDER BY Id ASC";

		using var conn = new SqlConnection(connectionString);
		await conn.OpenAsync();
		using var cmd = new SqlCommand(query, conn);
		cmd.Parameters.AddWithValue("@BatchSize", batchSize);
		cmd.Parameters.AddWithValue("@LastId", lastId);

		using var reader = await cmd.ExecuteReaderAsync();
		while (await reader.ReadAsync())
		{
			results.Add(new DbRow
			{
				Id = reader.GetInt64(0),
				EventType = reader.GetString(1),
				Source = reader.GetString(2),
				Timestamp = reader.GetDateTime(3),
				Data = reader.IsDBNull(4) ? null : reader.GetString(4),
				Properties = reader.IsDBNull(5) ? null : reader.GetString(5)
			});
		}
		return results;
	}

	private class DbRow
	{
		public long Id { get; set; }
		public string EventType { get; set; } = string.Empty;
		public string Source { get; set; } = string.Empty;
		public DateTime Timestamp { get; set; }
		public string? Data { get; set; }
		public string? Properties { get; set; }
	}
}
