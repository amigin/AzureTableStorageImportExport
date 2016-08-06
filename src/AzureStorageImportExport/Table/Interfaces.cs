using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageImportExport.Table
{
    public interface IAzureTableStream
    {
        IEnumerable<DynamicTableEntity> GetEntities();

        string TableName { get; }
    }


    /// <summary>
    /// Interface to get access to the data source    
    /// </summary>
    public interface IAzureTableStorageStream
    {
        /// <summary>
        /// Get tables from the source one by one
        /// </summary>
        /// <param name="tableNames">Empty means all tables from the source.</param>
        /// <returns></returns>
        IEnumerable<IAzureTableStream> GetTables(params string[] tableNames);
    }


    public static class TableStorageUtils
    {

        public static bool CanTableBeYelded(this string[] tableNames, string tableName)
        {
            if (tableNames.Length == 0)
                return true;

            return tableNames.Any(name => name == tableName);
        }
    }

}
