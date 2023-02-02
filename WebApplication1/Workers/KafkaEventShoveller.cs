using System.Globalization;
using System.Text;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Confluent.Kafka;
using KafkaAndSqsShoveller;
using Microsoft.Extensions.Options;
using nup.kafka;
using nup.kafka.Models;
using Serilog;
using Serilog.Context;
using WebApplication1.Models;
using MessageAttributeValue = Amazon.SQS.Model.MessageAttributeValue;

namespace WebApplication1.Workers;

public class KafkaEventShoveller : IHostedService
{
    private readonly ShovelToSqs _toSqs;
    private readonly IServiceScopeFactory _scope;
    private readonly IOptions<ServiceConfiguration> _configs;
    private CancellationTokenSource _cts;

    public KafkaEventShoveller(ShovelToSqs toSqs, IServiceScopeFactory scope, IOptions<ServiceConfiguration> configs)
    {
        _toSqs = toSqs;
        _scope = scope;
        _configs = configs;
        _cts = new CancellationTokenSource();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _toSqs.PushToSqsShovel(_cts.Token, () => _scope.CreateScope().ServiceProvider
            .GetRequiredService<IAmazonSimpleNotificationService>(), _configs.Value.TopicArn);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        return Task.CompletedTask;
    }
}