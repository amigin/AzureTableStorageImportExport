using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableStorageImportExport.Engine
{
    public interface IAzureTableStream
    {
        IEnumerable<DynamicTableEntity> GetEntities();

        string TableName { get; }
    }


    public interface IAzureTableStorageStream
    {
        IEnumerable<IAzureTableStream> GetTables();
    }

}
