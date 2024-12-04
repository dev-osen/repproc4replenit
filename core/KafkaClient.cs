using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNetEnv;

namespace RepProc4Replenit.Core;
    
public static class KafkaClient
{
    public static ProducerConfig ProducerConfig =>
        new ProducerConfig
        {
            BootstrapServers = $"{Env.GetString("KAFKA_HOST")}:{Env.GetString("KAFKA_PORT")}"
        };
    
    public static ConsumerConfig ConsumerConfig() => 
        new ConsumerConfig
        {
            BootstrapServers = $"{Env.GetString("KAFKA_HOST")}:{Env.GetString("KAFKA_PORT")}",
            GroupId = $"task-worker-{DateTime.Now.Ticks}", 
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
    
    public static AdminClientConfig AdminClientConfig => new AdminClientConfig
    {
        BootstrapServers = $"{Env.GetString("KAFKA_HOST")}:{Env.GetString("KAFKA_PORT")}",
    };


    public static async Task TopicChecker(List<string> topics, int maxConsumerCount)
    {
        using var adminClient = new AdminClientBuilder(KafkaClient.AdminClientConfig).Build();
        
        try
        { 
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            List<string> notExistTopics = topics.Where(x => !metadata.Topics.Any(y => y.Topic == x)).ToList();
            
            if (notExistTopics?.Any() ?? false)
                foreach (var topic in notExistTopics)
                    await adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification
                        {
                            Name = topic,
                            NumPartitions = maxConsumerCount,
                            ReplicationFactor = 1
                        }
                    });
 
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"[ERROR]: Topic create error >>> {e.Results[0].Error.Reason}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR]: Kafka error >>> {ex.Message}");
        }
        
    }
    
    
}