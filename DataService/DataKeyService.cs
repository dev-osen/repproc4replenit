using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;

namespace RepProc4Replenit.DataService;

public class DataKeyService: PostgreService<DataKey>
{
    public async Task<bool> CheckKey(string key)
    {
        string? check = await RuntimeControl.RedisDataKey.GetAsync(key);
        return !string.IsNullOrEmpty(check);
    }
}