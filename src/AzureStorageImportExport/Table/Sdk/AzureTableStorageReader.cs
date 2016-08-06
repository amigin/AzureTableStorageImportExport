using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageImportExport.Table.Sdk
{
    public class AzureTableScanner : IAzureTableStream
    {
        private readonly CloudTable _table;

        internal AzureTableScanner(CloudTable table)
        {
            _table = table;
        }

        public string TableName => _table.Name;

        public IEnumerable<DynamicTableEntity> GetEntities()
        {
            var rangeQuery = new TableQuery<DynamicTableEntity>();

            TableContinuationToken tableContinuationToken = null;
            do
            {
                var queryResponse = _table.ExecuteQuerySegmentedAsync(rangeQuery, tableContinuationToken).Result;
                tableContinuationToken = queryResponse.ContinuationToken;
                foreach (var entity in queryResponse.Results)
                     yield return entity;

            } while (tableContinuationToken != null);
        }


        public override string ToString()
        {
            return TableName;
        }

    }

    public class AzureTableStorageReader : IAzureTableStorageStream
    {
        private readonly string _connectionString;

        public AzureTableStorageReader(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IEnumerable<IAzureTableStream> GetTables(params string[] tableNames)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(_connectionString);

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            TableContinuationToken continuationToken = null;
            do
            {
                var segment = cloudTableClient.ListTablesSegmentedAsync(continuationToken).Result;
                continuationToken = segment.ContinuationToken;

                foreach (var table in segment.Results)
                {
                    if (tableNames.CanTableBeYelded(table.Name))
                        yield return new AzureTableScanner(table);
                }

            } while (continuationToken != null);
        }


        public static IAzureTableStorageStream CreateStream(string connectionString)
        {
            return new AzureTableStorageReader(connectionString);
        }


    }
}
