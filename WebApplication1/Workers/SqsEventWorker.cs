using KafkaAndSqsShoveller;
using KafkaAndSqsShoveller.SqsStuff;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;

namespace WebApplication1.Workers;

public class SqsEventWorker : IHostedService, IDisposable
{
    private KafkaWrapper _kafkaClient;
    private readonly ShovelToKafka _toKafka;
    private IServiceScopeFactory  _scope;
    private CancellationTokenSource _cts;

    public SqsEventWorker(ShovelToKafka toKafka, IServiceScopeFactory  scope)
    {
        _toKafka = toKafka;
        _scope = scope;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _toKafka.PushToKafka(cancellationToken,()=>_scope.CreateScope().ServiceProvider.GetRequiredService<IAWSSQSService>());
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