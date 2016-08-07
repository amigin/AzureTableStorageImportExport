
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzureStorageImportExport.Blob.Sdk
{


    public class AzureBlobContainer : IBlobContainerStream
    {

        private CloudBlobContainer _cloudBlobClient;

        public AzureBlobContainer(CloudBlobContainer cloudBlobClient){
           _cloudBlobClient = cloudBlobClient;
        }

        public string Name => _cloudBlobClient.Name;


        public IEnumerable<CloudBlob> GetBlobs()
        {
            BlobContinuationToken continuationToken = null;
            do
            {
                var blobsSegment = _cloudBlobClient.ListBlobsSegmentedAsync(continuationToken).Result;
                continuationToken = blobsSegment.ContinuationToken;
                foreach (var entity in blobsSegment.Results){
                     var blob = _cloudBlobClient.GetBlobReference(entity.Uri.ToString());
                     yield return blob;

                }

            } while (continuationToken != null);
        }


    }


    public class AzureBlobStorageReader : IBlobsStorageSteram
    {

        private string _connectionString;
        private AzureBlobStorageReader(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IEnumerable<IBlobContainerStream> GetContainers()
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(_connectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            BlobContinuationToken continuationToken = null;
            do
            {
                var segment = cloudBlobClient.ListContainersSegmentedAsync(continuationToken).Result;
                continuationToken = segment.ContinuationToken;

                foreach (var container in segment.Results)
                {
                   yield return new AzureBlobContainer(container);
                }

            } while (continuationToken != null);

        }
    }


}