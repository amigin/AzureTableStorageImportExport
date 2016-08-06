using System;
using TableStorageImportExport.Engine;

namespace TableStorageImportExport
{
    public class Program
    {
        public static void Main(string[] args)
        {

            var inputParams = InputParamsModel.ParseFromConsoleParams(args);

            if (inputParams == null)
            {
                PrintDescription();
                return;
            }

            if (!ValidateInputModel(inputParams))
                return;

            switch (inputParams.Operation)
            {
                case OperationType.Download:
                    AzureTableStorageDownloader.DownloadFromDb(inputParams.ConnectionString, inputParams.FileName);
                    break;
                case OperationType.Upload:
                    AzureTableStorageUploader.UploadToDb(inputParams.ConnectionString, inputParams.FileName);
                    break;
            }


            Console.WriteLine("Done");
            Console.ReadLine();
        }



        private static bool ValidateInputModel(InputParamsModel inputParams)
        {
            if (inputParams.FileName == null)
            {
                Console.WriteLine("Please specify filename:");
                Console.WriteLine("  fn:z:\\myfile.xml");
                Console.ReadLine();
                return false;
            }

            if (inputParams.ConnectionString == null)
            {
                Console.WriteLine("Please specify connectionstring:");
                Console.WriteLine("  cs:[connectionstring]");
                Console.ReadLine();
                return false;
            }


            if (inputParams.Operation == null)
            {
                Console.WriteLine("Please specify operation:");
                Console.WriteLine("  op:d - to download to file");
                Console.WriteLine("  op:u - to upload from file");
                Console.ReadLine();
                return false;
            }


            return true;
        }

        private static void PrintDescription()
        {
            Console.WriteLine("Azure Table storage database exporter/importer");
            Console.WriteLine("Format: op:[d/u] cs:[connectionstring] fn:filename.xml <tb:tablename|tablename2|tablename3>");
            Console.WriteLine("Parameters:");
            Console.WriteLine("op: operation. d - Download database to file, u - Upload database from file");
            Console.WriteLine("cs: Azure table storage connection string");
            Console.WriteLine("fn: filename to download or upload");
            Console.WriteLine("tb: tables to download. If parameter is absent - all tables are downloaded");

            Console.ReadLine();
        }

    }

}
