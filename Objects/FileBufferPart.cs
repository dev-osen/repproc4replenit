namespace RepProc4Replenit.Objects;

public class FileBufferPart
{
    public int Index { get; set; }
    public long Start { get; set; }
    public long End { get; set; }
    public long ChunkSize { get; set; }  
    public int LineCount { get; set; }
}