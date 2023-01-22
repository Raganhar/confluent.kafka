using nup.kafka;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;
using WebApplication1.SqsStuff;

namespace WebApplication1.Workers;

public class SqsEventWorker : IHostedService, IDisposable
{
    private KafkaWrapper _kafkaClient;
    private IServiceScopeFactory  _scope;
    private CancellationTokenSource _cts;

    public SqsEventWorker(KafkaWrapper kafkaClient, IServiceScopeFactory  scope)
    {
        _kafkaClient = kafkaClient;
        _scope = scope;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        using (LogContext.PushProperty("worker","sqsWorker"))
        {
            ThreadPool.QueueUserWorkItem(state =>
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    Log.Information("Retrieving SQS messages");
                    var sqs = _scope.CreateScope().ServiceProvider.GetRequiredService<IAWSSQSService>();
                    var allMessagesAsync = sqs.GetAllMessagesAsync();
                    Task.WaitAll(allMessagesAsync);
                    var allMessages = allMessagesAsync.Result;
                    Log.Information("found {msgCount}", allMessages.Count);
                    allMessages.ForEach(x =>
                    {
                        Log.Information("sending msg to kafka");
                        _kafkaClient.Send(x.UserDetail).Wait();
                        Log.Information("sendt msg to kafka");
                        sqs.DeleteMessageAsync(new DeleteMessage
                        {
                            ReceiptHandle = x.ReceiptHandle
                        });
                    });
                }
            }, "sqsworker");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}