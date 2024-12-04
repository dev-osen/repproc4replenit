namespace RepProc4Replenit.DataModels;

public class Replenishment
{
    public int Id { get; set; }
    
    public string TransactionKey { get; set; }
    public string CustProdKey { get; set; }
    
    public string UserReferenceId { get; set; }
    public long ProductId { get; set; }
    public DateTime TransactionDate { get; set; }
    
    public double CalculatedDuration { get; set; }
    public DateTime ReplenishmentDate { get; set; }
}