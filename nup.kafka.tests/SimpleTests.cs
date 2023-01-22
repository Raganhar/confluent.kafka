using System.Globalization;

namespace nup.kafka.tests;

public class SimpleTests
{
    [Test]
    public void TimeStampFormat()
    {
        Console.WriteLine(DateTime.Now.ToString("o"));
    }
}