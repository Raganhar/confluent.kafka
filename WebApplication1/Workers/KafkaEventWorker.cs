using nup.kafka;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;
using WebApplication1.SqsStuff;

namespace WebApplication1.Workers;

public class KafkaEventWorker : IHostedService, IDisposable
{
    private KafkaWrapperConsumer _consumer;
    private IServiceScopeFactory _scope;
    private CancellationTokenSource _cts;

    public KafkaEventWorker(KafkaWrapperConsumer consumer, IServiceScopeFactory scope)
    {
        _consumer = consumer;
        _scope = scope;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        using (LogContext.PushProperty("worker","KafkaEventWorker"))
        {
            _consumer.Consume(_cts.Token, (UserDetail e) =>
            {
               Log.Information("Kafka received and processed {@event}",e);
                // _scope.CreateScope().ServiceProvider.GetRequiredService<AWSSQSService>().PostMessageAsync(e).Wait();
            });
        }
        
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        return  Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}