using Confluent.Kafka;
using DotNetEnv;

namespace RepProc4Replenit.Core;
    
public static class KafkaClient
{
    public static ProducerConfig ProducerConfig =>
        new ProducerConfig
        {
            BootstrapServers = $"{Env.GetString("KAFKA_HOST")}:{Env.GetString("KAFKA_PORT")}",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha512, 
            SaslUsername = Env.GetString("KAFKA_USER"),
            SaslPassword = Env.GetString("KAFKA_PASS"),
        };
    
    public static ConsumerConfig ConsumerConfig() => 
        new ConsumerConfig
        {
            BootstrapServers = $"{Env.GetString("KAFKA_HOST")}:{Env.GetString("KAFKA_PORT")}",
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha512, 
            SaslUsername = Env.GetString("KAFKA_USER"),
            SaslPassword = Env.GetString("KAFKA_PASS"),
            GroupId = $"task-worker-{DateTime.Now.Ticks}", 
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
}