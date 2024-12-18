using StackExchange.Redis;
using DotNetEnv;

namespace RepProc4Replenit.Core;


public enum RedisDataTypesEnum
{
    PreData = 1,
    CustomerProduct = 2,
    CustomerProductChecker = 3,
    Transaction = 4,
    TransactionChecker = 5,
    TransactionWorker = 6,
    TransactionRaw = 7,
    Product = 8,
    ProductList = 9,
    TaskError = 10,
    Log = 11,
}


public static class RedisConn
{
    public static ConnectionMultiplexer Connection { get; set; }

    public static async Task Connect()
    {
        string host = Env.GetString("REDIS_HOST");
        string port = Env.GetString("REDIS_PORT");
        string password = Env.GetString("REDIS_PASS");

        string connectionString = $"{host}:{port},password={password}";
        
        Connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
    }
}



public class RedisClient : IDisposable
{
    
    public readonly IDatabase RedisDatabase;
    public RedisClient(RedisDataTypesEnum redisType)
    {  
        RedisDatabase = RedisConn.Connection.GetDatabase((int)redisType);
    }
    
    public async Task<bool> KeyExistsAsync(string key) => await RedisDatabase.KeyExistsAsync(key);
    
    public async Task<bool> SetAsync(string key, string value, TimeSpan? expiry = null) =>  await RedisDatabase.StringSetAsync(key, value, expiry);
    
    
    public async Task<string?> GetAsync(string key)
    {
        var value = await RedisDatabase.StringGetAsync(key);
        return value.HasValue ? value.ToString() : null;
    }
    
    public async Task<bool> DeleteAsync(string key) => await RedisDatabase.KeyDeleteAsync(key);
    
    public IEnumerable<string> GetKeys(string pattern = "*")
    {
        var server = RedisConn.Connection.GetServer(RedisConn.Connection.GetEndPoints()[0]);
        foreach (var key in server.Keys(pattern: pattern))
        {
            yield return key.ToString();
        }
    }
    
    public async Task<bool> HashSetAsync(string hashKey, string field, string value) => await RedisDatabase.HashSetAsync(hashKey, field, value);
    
    public async Task<string?> HashGetAsync(string hashKey, string field)
    {
        var value = await RedisDatabase.HashGetAsync(hashKey, field);
        return value.HasValue ? value.ToString() : null;
    }
    
    public async Task<long> ListRightPushAsync(string listKey, string value) => await RedisDatabase.ListRightPushAsync(listKey, value);
    
    public async Task ListRightPushAsync(string listKey, List<string> data)
    {   
        IBatch batch = RedisDatabase.CreateBatch();
        foreach (var item in data)
            await batch.ListRightPushAsync(listKey, item);
        
        batch.Execute();
    }
    
    public async Task<string?> ListLeftPopAsync(string listKey)
    {
        var value = await RedisDatabase.ListLeftPopAsync(listKey);
        return value.HasValue ? value.ToString() : null;
    }
    
    public async Task<List<string>> GetList(string listKey, int pageIndex, int pageSize)
    { 
        long start = pageIndex * pageSize;
        long end = start + pageSize - 1;

        var result = await RedisDatabase.ListRangeAsync(listKey, start, end);
        return result.Select(x => x.ToString()).ToList();
    }
    
    
    

    public async Task<bool> SetAddAsync(string setKey, string value) => await RedisDatabase.SetAddAsync(setKey, value);
    
    
    public async Task<IEnumerable<string>> SetMembersAsync(string setKey)
    {
        var members = await RedisDatabase.SetMembersAsync(setKey);
        var result = new List<string>();

        foreach (var member in members)
        {
            result.Add(member.ToString());
        }

        return result;
    } 

    public void Dispose()
    {
        // if(RedisConn.Connection?.IsConnected ?? false)
        //     RedisConn.Connection?.Dispose();
            
        GC.SuppressFinalize(this);
    }

    ~RedisClient(){
        this.Dispose();
    }
}

