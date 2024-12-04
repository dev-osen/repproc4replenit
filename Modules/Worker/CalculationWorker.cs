using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using DotNetEnv;
using RepProc4Replenit.Core;
using RepProc4Replenit.DataModels;
using RepProc4Replenit.DataService;
using RepProc4Replenit.Objects;
using StackExchange.Redis;

namespace RepProc4Replenit.Modules.Worker;

public class CalculationWorker: IDisposable
{ 
 
    public ReplenishmentService ReplenishmentService { get; set; }
    
    public ConcurrentBag<string> Lines { get; set; }
    public ConcurrentBag<string> ReplenishmentInsertLines { get; set; }
    public ConcurrentBag<string> ReplenishmentUpdateLines { get; set; }
    
      
    
    public IBatch RedisProductBatch { get; set; } 
    public IBatch RedisProductListBatch { get; set; } 
    
    
      
    public string CustomerProductKey = RuntimeControl.CustomerProductKey();
    public string ProductKey = RuntimeControl.ProductKey();
    
    
    
    
    
    public async Task Run(TaskWorker taskWorker)
    {
        try
        {
            Lines = new ConcurrentBag<string>(await RuntimeControl.RedisCustomerProduct.GetList(CustomerProductKey, taskWorker.PartIndex, taskWorker.PartSize));
            ReplenishmentInsertLines = new ConcurrentBag<string>();
            RedisProductBatch = RuntimeControl.RedisProduct.RedisDatabase.CreateBatch();
            RedisProductListBatch = RuntimeControl.RedisProductList.RedisDatabase.CreateBatch();
            ReplenishmentService = new ReplenishmentService();
         
            List<(int, int)> subParts = new List<(int, int)>();

            int threadMaxRowCount = Env.GetInt("CALC_THREAD_MAX_LINE_COUNT");
            int partCount = Lines.Count / threadMaxRowCount;
            for(int i = 0; i < partCount; i++)
                subParts.Add((i *  threadMaxRowCount, threadMaxRowCount));
        
            if(Lines.Count % threadMaxRowCount > 0)
                subParts.Add((partCount * threadMaxRowCount, Lines.Count % threadMaxRowCount));

        
            Parallel.ForEach(subParts, subPart => this.RunSubTask(subPart.Item1, subPart.Item2).Wait());
 
            RedisProductBatch.Execute();
            RedisProductListBatch.Execute();

            if (ReplenishmentInsertLines.Count > 0)
            {
                StringBuilder replenishmentInsertLines = new StringBuilder();
                foreach (string replenishmentInsertLine in ReplenishmentInsertLines)
                    replenishmentInsertLines.AppendLine(replenishmentInsertLine);
            
                await ReplenishmentService.BulkInsertWithStringBuilder(replenishmentInsertLines, "TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate, CalculatedDuration, ReplenishmentDate");
            }

            if (ReplenishmentUpdateLines.Count > 0)
            {
                StringBuilder replenishmentUpdateLines = new StringBuilder();
                foreach (string replenishmentUpdateLine in ReplenishmentUpdateLines)
                    replenishmentUpdateLines.AppendLine(replenishmentUpdateLine);
            
                await ReplenishmentService.BulkUpdateWithStringBuilder(replenishmentUpdateLines, "Id, TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate, CalculatedDuration, ReplenishmentDate");
            }
        }
        catch (AggregateException ex)
        {
            foreach (var innerEx in ex.InnerExceptions)
                await RuntimeControl.ErrorLog(taskWorker.TaskId, innerEx);
        }
        catch (Exception e)
        {
            await RuntimeControl.ErrorLog(taskWorker.TaskId, e);
        }
    }



    public async Task RunSubTask(int startIndex, int size)
    { 
        HashEntry[]? custProdTransactions;
        
        List<DateTime> dates;
        List<(string, DateTime)> lines;
        DateTime[] sortedDates;
        
        
        string lineId = string.Empty, custProdKey = string.Empty, transactionKey = string.Empty, userReferenceId = string.Empty, transactionDate = string.Empty, productId = string.Empty, replenishmentDate = string.Empty;
        string dateRaw = string.Empty; 
        string? lineValue = string.Empty;
        double calculatedDuration;
        
        
        for(int i = startIndex; i < startIndex + size; i++)
        {
            custProdKey = Lines.ElementAt(i);
            custProdTransactions = await RuntimeControl.RedisTransaction.RedisDatabase.HashGetAllAsync(custProdKey);
            
            if(custProdTransactions.Length <= 0)
                continue;
            
            lines = new List<(string, DateTime)>();
            dates = new List<DateTime>();
            foreach (var entry in custProdTransactions)
            {
                transactionKey = entry.Name.ToString(); 
                dateRaw = entry.Value.ToString();
                lineValue = await RuntimeControl.RedisTransactionRaw.GetAsync(transactionKey);

                if (string.IsNullOrEmpty(lineValue))
                    continue;

                DateTime dateValue = DateTime.ParseExact(dateRaw, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                
                lines.Add((lineValue, dateValue));
                dates.Add(dateValue);
            }
            
            sortedDates = dates.OrderBy(t => t).ToArray(); 
            List<int> dayDifferences = new List<int>(sortedDates.Length - 1);
            for (int j = 1; j < sortedDates.Length; j++)
                dayDifferences.Add((sortedDates[j] - sortedDates[j - 1]).Days);
            
            calculatedDuration = dayDifferences.Average();

            foreach ((string, DateTime) line in lines)
            {
                var columns = line.Item1.Split(','); // Id, TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate
                lineId = columns[0];
                transactionKey = columns[1];
                userReferenceId = columns[3];
                productId = columns[4];
                transactionDate = columns[5];
                replenishmentDate = line.Item2.AddDays(calculatedDuration).ToString("yyyy-MM-dd HH:mm:ss");
                
                
                // Id, TransactionKey, CustProdKey, UserReferenceId, ProductId, TransactionDate, CalculatedDuration, ReplenishmentDate
                if(lineId == "0")
                    ReplenishmentInsertLines.Add($"{transactionKey},{custProdKey},{userReferenceId},{productId},{transactionDate},{calculatedDuration},{replenishmentDate}");
                else
                    ReplenishmentUpdateLines.Add($"{lineId},{transactionKey},{custProdKey},{userReferenceId},{productId},{transactionDate},{calculatedDuration},{replenishmentDate}");
            }
             
            await RedisProductBatch.HashSetAsync($"{productId}:durations", custProdKey, calculatedDuration.ToString());
            
            if (!(await RuntimeControl.RedisProductList.KeyExistsAsync($"{productId}:data")))
            {
                await RedisProductListBatch.SetAddAsync($"{productId}:data", "0");   
                await RedisProductBatch.ListRightPushAsync(ProductKey, productId);
            }
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    
    ~CalculationWorker() => Dispose();
}