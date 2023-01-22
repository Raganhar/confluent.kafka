using System.Text;

namespace nup.kafka;

public static class KafkaSupportExtensions
{
    public static string AsString(this byte[] data)
    {
        return Encoding.UTF8.GetString(data);
    }
    public static string ToCorrectStringFormat(this DateTime date)
    {
        return date.ToString(KafkaConsts.DatetimeFOrmat);
    }
}