namespace RepProc4Replenit.Objects;

public class QueItem<T> where T : class
{
    public QueItem(T item)
    {
        Key = Guid.NewGuid().ToString("N");
        Item = item;
    }
    
    public string Key { get; set; }
    public T Item { get; set; }
}