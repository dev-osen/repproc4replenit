namespace RepProc4Replenit.Objects;

public class CalculatedItem
{
    public long DbRowId { get; set; }
    public string UserReferenceId { get; set; }
    public long ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime TransactionDate { get; set; }
    public double CalculatedDuration { get; set; }
    public DateTime ReplenishmentDate { get; set; }
}