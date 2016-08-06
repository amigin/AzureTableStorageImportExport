using System;
using TableStorageImportExport.Engine;
using TableStorageImportExport.Engine.AzureStorage;
using TableStorageImportExport.Engine.Xml;

namespace TableStorageImportExport
{
    public class Program
    {
        public static void Main(string[] args)
        {

            Console.WriteLine("Params count: "+args.Length);

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
                    AzureTableStorageReader.CreateStream(inputParams.ConnectionString).WriteToFile(inputParams.FileName);
                    break;
                case OperationType.Upload:
                    AzureStorageXmlReader.CreateStream(inputParams.FileName).WriteToDb(inputParams.ConnectionString);
                    break;
            }

            Console.WriteLine("Done");
        }



        private static bool ValidateInputModel(InputParamsModel inputParams)
        {

            if (inputParams.Operation == null)
            {
                Console.WriteLine("Please specify operation:");
                Console.WriteLine("  op:d - to download to file");
                Console.WriteLine("  op:u - to upload from file");
                return false;
            }


            if (inputParams.FileName == null)
            {
                Console.WriteLine("Please specify filename:");
                Console.WriteLine("  fn:z:\\myfile.xml");
                return false;
            }

            if (inputParams.ConnectionString == null)
            {
                Console.WriteLine("Please specify connectionstring:");
                Console.WriteLine("  cs:[connectionstring]");
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
            Console.WriteLine("cs: Azure table storage connection string. ;-symbol can be substitued as _");
            Console.WriteLine("fn: filename to download or upload");
            Console.WriteLine("tb: tables to download. If parameter is absent - all tables are downloaded");

        }

    }

}
