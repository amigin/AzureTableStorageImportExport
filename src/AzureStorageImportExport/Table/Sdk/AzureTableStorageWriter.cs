using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageImportExport.Table.Sdk
{
    public static class AzureTableStorageWriter
    {

        public static void WriteToDb(this IAzureTableStorageStream tableStorageStream, string connectionString)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            foreach (var table in tableStorageStream.GetTables())
            {
                Console.WriteLine("Uploading table:" + table.TableName);
                table.SaveTable(cloudTableClient);
            }

        }


        private static void SaveTable(this IAzureTableStream stream, CloudTableClient tableClient)
        {
            var tableRef = tableClient.GetTableReference(stream.TableName);
            tableRef.CreateIfNotExistsAsync().Wait();

            var entitiesToSave = new List<DynamicTableEntity>();

            foreach (var entity in stream.GetEntities())
            {
                if (entitiesToSave.Count > 0)
                    if (entitiesToSave[0].PartitionKey != entity.PartitionKey)
                        entitiesToSave.SaveBatch(tableRef);
                entitiesToSave.Add(entity);
                if (entitiesToSave.Count == 50)
                    entitiesToSave.SaveBatch(tableRef);

            }

            if (entitiesToSave.Count > 0)
                entitiesToSave.SaveBatch(tableRef);

        }


        private static void SaveBatch(this ICollection<DynamicTableEntity> entities, CloudTable tableRef)
        {
            var c = entities.Count;

            var batch = new TableBatchOperation();
            foreach (var entity in entities)
                batch.InsertOrReplace(entity);
            tableRef.ExecuteBatchAsync(batch).Wait();

            entities.Clear();
            Console.WriteLine($"Saved {c} entities");
        }

    }
}
