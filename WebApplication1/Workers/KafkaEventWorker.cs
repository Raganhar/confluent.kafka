using nup.kafka;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;

namespace WebApplication1.Workers;

public class KafkaEventWorker : IHostedService, IDisposable
{
    private KafkaWrapperConsumer _consumer;
    private CancellationTokenSource _cts;

    public KafkaEventWorker(KafkaWrapperConsumer consumer)
    {
        _consumer = consumer;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        using (LogContext.PushProperty("worker", "KafkaEventWorker"))
        {
            try
            {
                _consumer.Consume(_cts.Token, (UserDetail e) =>
                {
                    Log.Information("Kafka received and processed {@event}", e);
                });
            }
            catch (Exception e)
            {
                Log.Error("unhandled exception when trying to consume {@exception}",e);
            }
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