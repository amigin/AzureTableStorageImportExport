namespace TableStorageImportExport
{
    public enum OperationType
    {
        Download, Upload
    }

    public class InputParamsModel
    {

        public string ConnectionString { get; internal set; }
        public string FileName { get; internal set; }
        public OperationType? Operation { get; internal set; }


        public static InputParamsModel ParseFromConsoleParams(string[] args)
        {
            return ParamModelLoader.ParseFromParams(args);
        }

    }




    public static class ParamModelLoader
    {
        private const string OperationPrefix = "op:";
        private const string FileNamePrefix = "fn:";
        private const string ConnStringPrefix = "cs:";


        private static string ReadParamValue(string arg, string prefix)
        {
            return arg.Substring(prefix.Length, arg.Length - prefix.Length);
        }


        private static OperationType? ParseOperationType(string arg, string prefix)
        {
            var opTypeString = ReadParamValue(arg, prefix);

            switch (opTypeString)
            {
                case "d":
                    return OperationType.Download;

                case "u":
                    return OperationType.Upload;
            }

            return null;
        }



        internal static InputParamsModel ParseFromParams(string[] args)
        {

            if (args.Length == 0)
            return null;

            var result = new InputParamsModel();

            foreach (var arg in args)
            {

                if (arg.StartsWith(OperationPrefix))
                {
                    result.Operation = ParseOperationType(arg, OperationPrefix);
                }
                else if (arg.StartsWith(FileNamePrefix))
                {
                    result.FileName = ReadParamValue(arg, FileNamePrefix);
                }
                else if (arg.StartsWith(ConnStringPrefix))
                {
                    result.ConnectionString = ReadParamValue(arg, FileNamePrefix).Replace("_",";");
                }

            }


            return result;

        }

    }

}
