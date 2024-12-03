namespace RepProc4Replenit.DataModels;

public class Product
{
    public int Id { get; set; }
    public long DbRowId { get; set; }
    public string UserReferenceId { get; set; }
    public int Quantity { get; set; }
    public string TransactionDate { get; set; }
}