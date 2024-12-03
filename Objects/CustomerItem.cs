namespace RepProc4Replenit.Objects;

public class CustomerItem
{
    public long DbRowId { get; set; }
    public long ProductId { get; set; }
    public int Quantity { get; set; }
    public string TransactionDate { get; set; }
}