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

        mock.ReceivedWithAnyArgs().Get(null,null);
        mock.DidNotReceiveWithAnyArgs().AddEvent(null);
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
        mock.ReceivedWithAnyArgs().Get(null,null);
        mock.ReceivedWithAnyArgs().AddEvent(null);
    }
    
    [Test]
    public void ShouldFailSuccessiveMessagesOfAggregateMessageWithSamePartionKey()
    {
        var eventProcesser = new EventProcesser();
        var mock = NSubstitute.Substitute.For<IDaoLayer>();
        mock.Get(Arg.Any<TopicPartitionOffset>(), Arg.Any<string>()).ReturnsForAnyArgs(null as KafkaMessage);
        mock.DidPreviousRelatedEntityFail(Arg.Any<TopicPartitionOffset>(), Arg.Any<string>()).ReturnsForAnyArgs(true);
        var message = EmptySampleMessage();
        message.Message.Headers = KafkaWrapper.AddHeaders(new Dictionary<string, string>(){{KafkaConsts.PartitionKey,"asd"}});
        var hasBeenCalled = false;
        
        eventProcesser.ProcessMessage((string e) => { hasBeenCalled = true;},message,() => {},mock,"");
        
        hasBeenCalled.Should().BeFalse();
        mock.ReceivedWithAnyArgs().Get(null,null);
        mock.ReceivedWithAnyArgs().DidPreviousRelatedEntityFail(null,null);
        mock.Received().AddEvent(Arg.Is<KafkaMessage>(u => u.ProcessedSuccefully == false));
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