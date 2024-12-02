namespace RepProc4Replenit.Objects;

public class PurchaseItem{
    public long UserId { get; set; }
    public string ProductId { get; set; }
    public DateOnly PurchaseDate { get; set; }
    public double CalculatedDuration { get; set; }
    public DateOnly ReplenishmentDate { get; set; }
}