using System.Data;
using System.Text;
using DotNetEnv;
using Npgsql;

namespace RepProc4Replenit.Core;

public static class PostgreClient
{ 
    public static async Task<NpgsqlConnection> Connection()
    {
        string host = Env.GetString("POSTGRE_HOST");
        string port = Env.GetString("POSTGRE_PORT");
        string user = Env.GetString("POSTGRE_USER");
        string password = Env.GetString("POSTGRE_PASS");
        string database = Env.GetString("POSTGRE_DB");
           
        NpgsqlDataSourceBuilder dataSourceBuilder = new NpgsqlDataSourceBuilder($"Host={host}:{port};Database={database};Username={user};Password={password}");
        NpgsqlDataSource dataSource = dataSourceBuilder.Build();
        NpgsqlConnection conn = await dataSource.OpenConnectionAsync();

        return conn;
    }
     
}