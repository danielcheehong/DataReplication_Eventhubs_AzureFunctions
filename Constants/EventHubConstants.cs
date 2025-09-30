namespace EventHubDataReplication.Constants
{
    public static class EventHubConstants
    {
        public const string EventHubName = "events-source";
        public const string ConsumerGroupCosmos = "TargetCosmosReplicator";
        public const string ConsumerGroupSql = "TargetSqlReplicator";
        public const string ConsumerGroupTable = "TargetTableReplicator";
    }
}
