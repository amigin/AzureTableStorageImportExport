using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageImportExport.Engine
{
    public class AzureTableScanner
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

    }


    public static class AzureTableStorageScanner
    {

        public static IEnumerable<AzureTableScanner> DownloadFromDb(string connectionString)
        {


            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();


            TableContinuationToken continuationToken = null;
            do
            {
                var segment = cloudTableClient.ListTablesSegmentedAsync(continuationToken).Result;
                continuationToken = segment.ContinuationToken;

                foreach (var table in segment.Results)
                {
                    var tableScanner = new AzureTableScanner(table);
                    yield return tableScanner;
                }


            } while (continuationToken != null);
        }
    }
}
