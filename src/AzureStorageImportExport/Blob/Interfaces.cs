using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureStorageImportExport.Blob
{

    public interface IBlobContainerStream
    {
        IEnumerable<CloudBlob> GetBlobs();

        string Name {get;}
    }

    public interface IBlobsStorageSteram
    {
        IEnumerable<IBlobContainerStream> GetContainers();
    }

}