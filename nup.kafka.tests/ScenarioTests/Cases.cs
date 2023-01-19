using Confluent.Kafka;
using FluentAssertions;
using NSubstitute;
using nup.kafka.DatabaseStuff;

namespace nup.kafka.tests.ScenarioTests;

public class DontProcessPreviousSuccessMessageTests
{
    [Test]
    public void DontProcessPreviousSuccessMessageTest()
    {
        var eventProcesser = new EventProcesser();
        var mock = NSubstitute.Substitute.For<IDaoLayer>();
        var successfullMessage = new KafkaMessage
        {
            ProcessedSuccefully = true
        };
        mock.Get(Arg.Any<TopicPartitionOffset>(), Arg.Any<string>()).ReturnsForAnyArgs(successfullMessage);
        var message = EmptySampleMessage();
        eventProcesser.ProcessMessage((string e)=>{Assert.Fail();},message,() => {},mock,"");
    }
    
    [Test]
    public void ProcessPreviousFailureTests()
    {
        var eventProcesser = new EventProcesser();
        var mock = NSubstitute.Substitute.For<IDaoLayer>();
        var successfullMessage = new KafkaMessage
        {
            ProcessedSuccefully = false
        };
        mock.Get(Arg.Any<TopicPartitionOffset>(), Arg.Any<string>()).ReturnsForAnyArgs(successfullMessage);
        var message = EmptySampleMessage();
        var hasBeenCalled = false;
        eventProcesser.ProcessMessage((string e) => { hasBeenCalled = true;},message,() => {},mock,"");
        hasBeenCalled.Should().BeTrue();
    }

    private static ConsumeResult<Ignore, string> EmptySampleMessage()
    {
        return new ConsumeResult<Ignore, string>()
        {
            Message = new Message<Ignore, string>()
            {
                Value = "",
                Headers = new Headers(),
            }
        };
    }
}