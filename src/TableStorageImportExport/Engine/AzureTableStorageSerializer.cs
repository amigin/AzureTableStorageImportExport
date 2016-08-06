using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace TableStorageImportExport.Engine
{

    public static class XmlConsts
    {
        public const string RootTag = "tableStorage";

        public const string TableTag = "Table";
        public const string EntityTag = "Entity";
        public const string EntityFieldTag = "Field";

        public const string TableNameAttr = "Name";

        public const string EntityTypeAttr = "Type";
        public const string EntityValueAttr = "Value";
        public const string EntityKeyAttr = "Key";

        public const string PartitionKeyAttr = "P";
        public const string RowKeyAttr = "R";
    }


    public static class TypeSerializer
    {
        public const string BinType = "Bin";
        public const string StrType = "String";
        public const string Int32Type = "Int32";
        public const string Int64Type = "Int64";
        public const string DateTimeType = "DateTime";
        public const string BoolType = "Bool";
        public const string DoubleType = "Double";


        public static DateTime ParseIsoDateTime(this string value)
        {
            value = value.Replace('_', ' ');
            DateTime result;
            var isOk = DateTime.TryParseExact(value, IsoDateTimeMask,
                CultureInfo.InvariantCulture, DateTimeStyles.None, out result);

            if (!isOk)
                throw new Exception("Invalid datetime ISO format " + value);

            return result;
        }

        public const string IsoDateTimeMask = "yyyy-MM-dd HH:mm:ss";

        public static string ToIsoDateTime(this DateTime src)
        {
            return src.ToString(IsoDateTimeMask);
        }


        internal static string EntityProprtyTypeToString(this object src)
        {
            if (src is IEnumerable<byte>)
                return BinType;

            var type = src.GetType();

            if (type == typeof(byte) || type == typeof(sbyte) || type == typeof(short) || type == typeof(ushort) ||
                type == typeof(int) || type == typeof(uint))
                return Int32Type;

            if (type == typeof(long) || type == typeof(ulong))
                return Int64Type;

            if (type == typeof(DateTime))
                return DateTimeType;

            if (type == typeof(bool))
                return BoolType;

            if (type == typeof(float) || type == typeof(double))
                return DoubleType;

            return StrType;

        }


        internal static string ToXmlString(this object src)
        {
            if (src == null)
                return "<<<null>>>";


            var asBytes = src as IEnumerable<byte>;
            if (asBytes != null)
                return Convert.ToBase64String(asBytes.ToArray());

            if (src is bool)
                return (bool)src ? "1" : "0";

            if (src is DateTime)
                return ((DateTime)src).ToIsoDateTime();



            var asString = src as string;
            if (asString != null)
            {
                var bytes = Encoding.UTF8.GetBytes(asString);
                return Convert.ToBase64String(bytes);
            }


            return Convert.ChangeType(src, typeof(string), CultureInfo.InvariantCulture).ToString();


        }

    }

}
