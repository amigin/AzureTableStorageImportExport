using System;
using System.IO;
using AzureStorageImportExport.Table.Sdk;
using AzureStorageImportExport.Table.Xml;

namespace TableStorageImportExport
{
    public class Program
    {
        public static void Main(string[] args)
        {


            try
            {
                Console.WriteLine("Params count: " + args.Length);

                var inputParams = InputParamsModel.ParseFromConsoleParams(args);

                if (inputParams == null)
                {
                    PrintDescription();
                    return;
                }

                if (!ValidateInputModel(inputParams))
                    return;


                DoMainStuff(inputParams);

            }
            finally
            {
                Console.WriteLine("Done");

                #if DEBUG
                Console.ReadLine();
                #endif

            }

        }

        private static void DoMainStuff(InputParamsModel inputParams)
        {
            try
            {
                switch (inputParams.Operation)
                {
                    case OperationType.Download:
                        using (var fileStream = new FileStream(inputParams.FileName, FileMode.Create))
                        {
                            AzureTableStorageReader.CreateStream(inputParams.ConnectionString).WriteToStream(fileStream);
                            fileStream.Flush();
                        }
                        break;
                    case OperationType.Upload:
                        using (var fileStream = new FileStream(inputParams.FileName, FileMode.Open))
                            AzureStorageXmlReader.CreateStream(fileStream).WriteToDb(inputParams.ConnectionString);
                        break;
                }

            }
            catch (Exception e)
            {

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.Message);
                Console.ResetColor();

            }
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
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Azure Table storage database exporter/importer");
            Console.ResetColor();
            Console.WriteLine("Format: op:[d/u] cs:[connectionstring] fn:filename.xml <tb:tablename|tablename2|tablename3>");
            Console.WriteLine("Parameters:");
            Console.WriteLine("op: operation. d - Download database to file, u - Upload database from file");
            Console.WriteLine("cs: Azure table storage connection string. ;-symbol can be substitued as _");
            Console.WriteLine("fn: filename to download or upload");
            Console.WriteLine("tb: tables to download. If parameter is absent - all tables are downloaded");

        }

    }

}
