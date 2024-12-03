namespace RepProc4Replenit.Objects;

public class ProductItem
{
    public long DbRowId { get; set; }
    public string UserReferenceId { get; set; }
    public int Quantity { get; set; }
    public string TransactionDate { get; set; }
}