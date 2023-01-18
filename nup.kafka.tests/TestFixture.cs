using Serilog;

namespace nup.kafka.tests;

public class TestFixture
{
    [SetUp]
    public void Setup()
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Seq("http://localhost:5341/").CreateLogger();
    }

    [TearDown]
    public void TearDown()
    {
        Log.CloseAndFlush();
    }
}