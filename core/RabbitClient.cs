using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using DotNetEnv; 
using Aspire.RabbitMQ.Client;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RepProc4Replenit.Objects;

namespace RepProc4Replenit.Core;

public static class RabbitClient
{ 
    public static IConnection? Connection { get; set; }
    
    
    public static IModel TaskChannel { get; set; }
    public static IModel ReplyChannel { get; set; }
    
    
    public static string TaskQueueName { get; set; }
    public static string ReplyQueueName { get; set; }
    
    
    public static ConcurrentBag<string> ReplyQueItemKeys { get; set; } = new ConcurrentBag<string>();
    
    
    public static void Connect()
    {
        TaskQueueName = Env.GetString("RABBIT_TASK_CHANNEL");
        ReplyQueueName = Env.GetString("RABBIT_REPLY_CHANNEL");
        
        
        ServiceCollection serviceCollection = new ServiceCollection();
        serviceCollection.AddSingleton<IRabbitConnection>(sp =>
            new RabbitConnection(
                hostName: Env.GetString("RABBIT_HOST"),
                port: Env.GetInt("RABBIT_PORT"),
                userName: Env.GetString("RABBIT_USER"),
                password: Env.GetString("RABBIT_PASS")
            ));

        var serviceProvider = serviceCollection.BuildServiceProvider();
        var rabbitConnection = serviceProvider.GetRequiredService<IRabbitConnection>();

        Connection = rabbitConnection.GetConnection();
        
        TaskChannel = Connection.CreateModel();
        TaskChannel.QueueDeclare(TaskQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
        
        ReplyChannel = Connection.CreateModel();
        ReplyChannel.QueueDeclare(ReplyQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
        
        
        if(RuntimeControl.ProgramMode != "consumer")
            SubscribeToReplyQueue(reply => ReplyQueItemKeys.Add(reply));
    }






    public static async Task ProduceAsync<T>(T data) where T : class
    {
        try
        {
            if (Connection == null || !Connection.IsOpen)
                Connect();

            QueItem<T> queItem = new QueItem<T>(data);
            byte[] body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(queItem));
            TaskChannel.BasicPublish(exchange: "", routingKey: TaskQueueName, basicProperties: null, body: body);

            while (!ReplyQueItemKeys.Any(t => t == queItem.Key))
            {
                RuntimeControl.ProgramCancellation.Token.ThrowIfCancellationRequested();
                await Task.Delay(500);
            }
        }
        catch (TaskCanceledException tce) { }
        catch (Exception e) { }
    }

    public static void Consume<T>(Action<T> callback, CancellationToken ct) where T : class
    {
        try
        {
            Task.Run(() =>
            {
                EventingBasicConsumer consumer = new EventingBasicConsumer(TaskChannel);
                consumer.Received += (model, ea) =>
                {
                    byte[] body = ea.Body.ToArray();
                    string message = Encoding.UTF8.GetString(body);
                    QueItem<T> data = JsonSerializer.Deserialize<QueItem<T>>(message);
            
                    try
                    {
                        callback(data.Item); 
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e); 
                    }
            
                    SendToReply(data.Key);
                };

                TaskChannel.BasicConsume(queue: TaskQueueName, autoAck: true, consumer: consumer);
            }, ct);
        }
        catch (TaskCanceledException tce){}
        catch (Exception e) {}
    }
    
    
    
    private static void SendToReply(string data)
    {
        if (Connection == null || !Connection.IsOpen)
            Connect();
         
        byte[] body = Encoding.UTF8.GetBytes(data);
        TaskChannel.BasicPublish(exchange: "", routingKey: TaskQueueName, basicProperties: null, body: body);
    }
    
    
    private static void SubscribeToReplyQueue(Action<string> callback)
    {
        EventingBasicConsumer consumer = new EventingBasicConsumer(TaskChannel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            callback(message); 
        };

        ReplyChannel.BasicConsume(queue: ReplyQueueName, autoAck: true, consumer: consumer);
    }
    
    
    
    
}