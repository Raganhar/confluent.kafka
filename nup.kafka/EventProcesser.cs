using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using nup.kafka.DatabaseStuff;
using Serilog;
using Serilog.Context;

namespace nup.kafka;
public class EventProcesser
{
    public void ProcessMessage<T>(Action<T> handler, ConsumeResult<Ignore, string> consumeResult, Action storeOffset, IDaoLayer daoLayer, string consumerIdentifier)
    {
        using (LogContext.PushProperty("TopicPartitionOffset", consumeResult.TopicPartitionOffset))
        {
            var recievedAtUtc = DateTime.UtcNow;
            var headers = GetHeaders(consumeResult);

            var partitionKey = headers.ContainsKey(KafkaConsts.PartitionKey)
                ? headers[KafkaConsts.PartitionKey]
                : null;
            var previouslyProcessedMessage =
                daoLayer.Get(consumeResult.TopicPartitionOffset, partitionKey);
            if (previouslyProcessedMessage?.ProcessedSuccefully == true)
            {
                Log.Information(
                    "Received message on topicPartition: {TopicPartitionOffset} which was already successfully processed",
                    consumeResult.TopicPartitionOffset);
                storeOffset();
                Log.Information("Stored offset (ignored the message)");
                return;
            }

            var kafkaMessage = new KafkaMessage
            {
                Partition = consumeResult.Partition.Value,
                OffSet = consumeResult.Offset.Value,
                FinishedProcessingAtUtc = DateTime.UtcNow,
                RecievedCreatedAtUtc = recievedAtUtc,
                PartitionKey = partitionKey,
                ProcessedSuccefully = true,
                Topic = consumeResult.Topic
            };

            if (partitionKey != null)
            {
                var previousAggregateEntityFailed =
                    daoLayer.DidPreviousRelatedEntityFail(consumeResult.TopicPartitionOffset, partitionKey);
                if (previousAggregateEntityFailed)
                {
                    Log.Information(
                        "Previous entity message failed to be processed, will not process this in order to guarantee order of execution for topicPartition: {TopicPartitionOffset}",
                        consumeResult.TopicPartitionOffset);
                    storeOffset();
                    kafkaMessage.ProcessedSuccefully = false;
                    kafkaMessage.ReasonText = "Previous entity message failed to be processed";
                    daoLayer.AddEvent(kafkaMessage);

                    Log.Information("Stored offset (ignored the message)");
                    return;
                }
            }

            try
            {
                var eventObj = JsonConvert.DeserializeObject<T>(consumeResult.Message.Value);
                Log.Information(
                    $"{consumerIdentifier} Received message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
                handler(eventObj);
                Log.Information(
                    $"Handled message at {consumeResult.TopicPartitionOffset}: {JsonConvert.SerializeObject(eventObj)}");
            }
            catch (Exception e)
            {
                Log.Error("Failed to process event at {TopicPartitionOffset}");
                SaveFailedMessage(kafkaMessage, e,daoLayer);
                throw;
            }

            try
            {
                // Store the offset associated with consumeResult to a local cache. Stored offsets are committed to Kafka by a background thread every AutoCommitIntervalMs. 
                // The offset stored is actually the offset of the consumeResult + 1 since by convention, committed offsets specify the next message to consume. 
                // If EnableAutoOffsetStore had been set to the default value true, the .NET client would automatically store offsets immediately prior to delivering messages to the application. 
                // Explicitly storing offsets after processing gives at-least once semantics, the default behavior does not.
                storeOffset();
                daoLayer.AddEvent(kafkaMessage);
            }
            catch (KafkaException e)
            {
                Log.Information($"Store Offset error: {e.Error.Reason}");
                SaveFailedMessage(kafkaMessage, e,daoLayer);
            }
        }
    }

    private void SaveFailedMessage(KafkaMessage kafkaMessage, Exception e, IDaoLayer daoLayer)
    {
        kafkaMessage.ProcessedSuccefully = false;
        kafkaMessage.ReasonText = e.Message;
        daoLayer.AddEvent(kafkaMessage);
    }

    private static Dictionary<string, string> GetHeaders(ConsumeResult<Ignore, string> consumeResult)
    {
        return
            consumeResult.Message.Headers?.ToDictionary(x => x.Key, x => Encoding.UTF8.GetString(x.GetValueBytes())) ??
            new Dictionary<string, string>();
    }
}