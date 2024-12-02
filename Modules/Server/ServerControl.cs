using Confluent.Kafka;
using RepProc4Replenit.Core;

namespace RepProc4Replenit.Modules.Server;

public static class ServerControl
{
    public static void Run()
    {
         IProducer<Null, string> Producer = new ProducerBuilder<Null, string>(KafkaClient.ProducerConfig).Build();
    }
    
    
    
}