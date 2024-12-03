namespace RepProc4Replenit.DataModels;

public class Customer
{
    public int Id { get; set; }
    public long DbRowId { get; set; }
    public long ProductId { get; set; }
    public int Quantity { get; set; }
    public string TransactionDate { get; set; }
}