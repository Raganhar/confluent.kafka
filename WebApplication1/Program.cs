using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using KafkaAndSqsShoveller;
using KafkaAndSqsShoveller.SqsStuff;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using nup.kafka;
using nup.kafka.DatabaseStuff;
using Serilog;
using WebApplication1;
using WebApplication1.Workers;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
Log.Logger = new LoggerConfiguration().Enrich.FromLogContext().WriteTo.Seq("http://localhost:5341/").CreateLogger();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var configuration = builder.Configuration;
builder.Services.Configure<ServiceConfiguration>(configuration);
var awsOptions = configuration.GetAWSOptions();
awsOptions.Credentials = new EnvironmentVariablesAWSCredentials();
builder.Services.AddDefaultAWSOptions(awsOptions);  
builder.Services.AddAWSService<IAmazonSimpleNotificationService>();  
builder.Services.AddAWSService<IAmazonSQS>();  
builder.Services.AddTransient<SqsStuff>();  
builder.Services.AddTransient<IAWSSQSService, AWSSQSService>();  
builder.Services.AddTransient<IAWSSQSHelper, AWSSQSHelper>();
builder.Services.AddSingleton<ShovelToSqs>();
builder.Services.AddSingleton<ShovelToKafka>();
var appName = "sqs_kafka_bridge";

builder.Services.AddSingleton<KafkaWrapper>(x =>
{
    var conf = x.GetRequiredService<IOptions<ServiceConfiguration>>().Value;
    return new KafkaWrapper(conf.BrokerList,appName,new ProducerOptions
    {
        PartitionCount = 30
    });
});
builder.Services.AddSingleton<KafkaWrapperConsumer>(x=>
{
    var conf = x.GetRequiredService<IOptions<ServiceConfiguration>>().Value;
    var kafkaWrapperConsumer = new KafkaWrapperConsumer(conf.BrokerList,appName,new EventProcesser(),"asd").WithDatabase(KafkaMysqlDbContext.ConnectionString);
    return kafkaWrapperConsumer;
});

// builder.Services.AddHostedService<KafkaEventWorker>();
builder.Services.AddHostedService<SqsEventWorker>();
builder.Services.AddHostedService<KafkaEventShoveller>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();
Log.Information("Started");
app.Run();