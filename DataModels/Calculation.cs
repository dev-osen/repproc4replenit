namespace RepProc4Replenit.DataModels;

public class Calculation
{
    public int Id { get; set; }
    public long DbRowId { get; set; }
    public string UserReferenceId { get; set; }
    public long ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime TransactionDate { get; set; }
    public double CalculatedDuration { get; set; }
    public DateTime ReplenishmentDate { get; set; }
}