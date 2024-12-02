using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNetEnv;

namespace RepProc4Replenit.Core;

public class RedisClient : IDisposable
{
    private readonly ConnectionMultiplexer RedisConnection;
    private readonly IDatabase RedisDatabase;

    public RedisClient()
    { 
        string host = Env.GetString("REDIS_HOST");
        string port = Env.GetString("REDIS_PORT");
        string password = Env.GetString("REDIS_PASSWORD");
        int database = 0;

        var connectionString = $"{host}:{port},password={password}";

        RedisConnection = ConnectionMultiplexer.Connect(connectionString);
        RedisDatabase = RedisConnection.GetDatabase(database);
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
        var server = RedisConnection.GetServer(RedisConnection.GetEndPoints()[0]);
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
    
    public async Task<string?> ListLeftPopAsync(string listKey)
    {
        var value = await RedisDatabase.ListLeftPopAsync(listKey);
        return value.HasValue ? value.ToString() : null;
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
        if(RedisConnection?.IsConnected ?? false)
            RedisConnection?.Dispose();
            
        GC.SuppressFinalize(this);
    }

    ~RedisClient(){
        this.Dispose();
    }
}

