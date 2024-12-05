using RabbitMQ.Client;

namespace RepProc4Replenit.Core;

public interface IRabbitConnection
{
    IConnection GetConnection();
}

public class RabbitConnection : IRabbitConnection
{
    private readonly ConnectionFactory _connectionFactory;

    public RabbitConnection(string hostName, int port, string userName, string password)
    {
        _connectionFactory = new ConnectionFactory
        {
            HostName = hostName,
            Port = port,
            UserName = userName,
            Password = password
        };
    }

    public IConnection GetConnection()
    {
        return _connectionFactory.CreateConnection();
    }
}