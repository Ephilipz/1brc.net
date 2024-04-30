using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc.csharp;

/// <summary>
/// A struct that represents a UTF-8 encoded span of bytes. This struct is used to avoid creating strings for each city name.
/// It lazily creates a string when the ToString method is called. Credit: @buybackoff
/// </summary>
unsafe struct UTF8Span : IEquatable<UTF8Span>
{
    private readonly byte* _ptr;
    private readonly int _length;
    private readonly ReadOnlySpan<byte> _span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            return new ReadOnlySpan<byte>(_ptr, _length);
        }
    }
    private string _string = "";

    public UTF8Span(byte* ptr, int length)
    {
        _ptr = ptr;
        _length = length;
    }

    // For hashing, we use the first 4 bytes of the string
    public override int GetHashCode()
    {
        // The magic number 820243 is the largest happy prime that contains 2024
        if (_length > 3)
            return (_length * 820243) ^ *(int*)_ptr;
        if (_length > 1)
            return *(short*)_ptr;
        if (_length > 0)
            return *_ptr;
        return 0;
    }

    public bool Equals(UTF8Span other)
    {
        // TODO : use SIMD for faster comparison
        return _span.SequenceEqual(other._span);
    }

    public override bool Equals(object? obj)
    {
        return obj is UTF8Span other && Equals(other);
    }

    public override string ToString()
    {
        if (_string.Length == 0)
            _string = new string((sbyte*)_ptr, 0, _length, Encoding.UTF8);
        return _string;
    }
}

struct Measurement
{
    public int Min = 999;
    public int Max = -999;
    public int Sum;
    public int Count;

    public Measurement(int min, int max, int sum, int count)
    {
        Min = min;
        Max = max;
        Sum = sum;
        Count = count;
    }

    public void Merge(Measurement other, bool isFirst = false)
    {
        if (isFirst || other.Min < Min)
            Min = other.Min;
        if (isFirst || other.Max > Max)
            Max = other.Max;
        Sum += other.Sum;
        Count += other.Count;
    }
}

unsafe class Program
{

    static void Main(string[] args)
    {
        var inputFile = Path.Combine("..", "1brc.data", "measurements-1000000000.txt");
        var version = "3";
        var resultsFile = $"results_{version}.txt";

        Console.WriteLine("Processing 1 billion measurements...");
        var startTime = Stopwatch.StartNew();

        // read file using mmap

        // pointer to the start of the file
        byte* ptr = (byte*)0;
        // filestream to get file size
        using var fileStream = new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.None, 1, FileOptions.None);
        var fileLength = fileStream.Length;
        // create memory mapped file
        using var mmf = MemoryMappedFile.CreateFromFile(fileStream, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true);
        // create view accessor
        using var accessor = mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
        using var vaHandle = accessor.SafeMemoryMappedViewHandle;
        vaHandle.AcquirePointer(ref ptr);

        // 2MB chunk size seems to be the sweet spot
        var chunkSize = 2 * 1024 * 1024;
        var chunkCount = (int)Math.Ceiling((double)fileLength / chunkSize);
        var tasks = new Task<Dictionary<UTF8Span, Measurement>>[chunkCount];

        int i = 0;
        var pos = 0L;
        while (pos < fileLength)
        {
            var (chunk_start, chunk_length) = (0L, 0L);
            
            // last chunk
            if (pos + chunkSize >= fileLength)
            {
                (chunk_start, chunk_length) = (pos, fileLength - pos);
                pos = fileLength;
            }
            else
            {
                // find newline character in next chunk and update the current chunk size to be that long
                var nextChunkStart = pos + chunkSize;
                var sp = new ReadOnlySpan<byte>(ptr + nextChunkStart, (int)chunkSize);

                // find next newline character
                var idx = sp.IndexOfAny((byte)'\n', (byte)'\r');

                // if the newline character is \r\n, skip 2 bytes, otherwise 1
                var stride = sp[idx] == (byte)'\n' ? 1 : 2;
                nextChunkStart += idx + stride;

                // update chunk size
                (chunk_start, chunk_length) = (pos, nextChunkStart - pos);
                pos = nextChunkStart;
            }
            // process chunk
            tasks[i++] = Task.Run(() => ProcessChunk(ptr + chunk_start, chunk_length));
        }

        // wait for all tasks to complete (blocking)
        Task.WaitAll(tasks);

        Dictionary<UTF8Span, Measurement>? finalMap = null;
        foreach (var task in tasks)
        {
            var chunkMap = task.Result;
            if (finalMap == null)
            {
                finalMap = chunkMap;
                continue;
            }

            // merge all result dictionaries into one final dictionary
            foreach (var kvp in chunkMap)
            {
                ref var measurement = ref CollectionsMarshal.GetValueRefOrAddDefault(finalMap, kvp.Key, out bool exists);
                if (exists)
                {
                    measurement.Merge(kvp.Value);
                }
                else
                {
                    measurement = kvp.Value;
                }
            }
        }

        Debug.Assert(finalMap != null);

        // output
        foreach (var kvp in finalMap.OrderBy(kvp => kvp.Key.ToString(), StringComparer.InvariantCulture))
        {
            var measurement = kvp.Value;
            var average = Math.Round((measurement.Sum / 10.0 / measurement.Count), 2);
            Console.WriteLine($"{kvp.Key} min: {measurement.Min / 10.0} max: {measurement.Max / 10.0} avg: {average}");
        }

        // results
        startTime.Stop();
        var elapsedTime = startTime.ElapsedMilliseconds / 1000.0;
        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"Total time: {elapsedTime} s");
        Console.ResetColor();
    }

    private static Dictionary<UTF8Span, Measurement> ProcessChunk(byte* ptr, long len)
    {
        var map = new Dictionary<UTF8Span, Measurement>();
        var pos = 0;

        while (pos < len)
        {
            var sp = new ReadOnlySpan<byte>(ptr + pos, (int)len);

            // find the first semicolon
            var sep = sp.IndexOf((byte)';');
            var cityName = new UTF8Span(ptr + pos, sep);

            // update measurement
            ref var measurement = ref CollectionsMarshal.GetValueRefOrAddDefault(map, cityName, out bool exists);
            sp = sp.Slice(sep + 1);
            var newLnIdx = sp.IndexOfAny((byte)'\n', (byte)'\r');
            var stride = sp[newLnIdx] == (byte)'\n' ? 1 : 2;
            var temp = ParseTemp(sp.Slice(0, newLnIdx));

            if (!exists || temp < measurement.Min)
                measurement.Min = temp;
            if (!exists || temp > measurement.Max)
                measurement.Max = temp;
            measurement.Sum += temp;
            measurement.Count++;

            pos += sep + newLnIdx + stride + 1;
        }

        return map;
    }

    /// <summary>
    /// Parses the temperature from the given span as an integer as it's faster than double parsing.
    /// </summary>
    private static int ParseTemp(ReadOnlySpan<byte> span)
    {
        var sign = 1;
        var temp = 0;
        for (var i = 0; i < span.Length; i++)
        {
            if (i == 0 && span[i] == (byte)'-')
            {
                sign = -1;
                continue;
            }
            if (span[i] == (byte)'.')
            {
                continue;
            }
            temp = temp * 10 + (span[i] - '0');
        }
        return temp * sign;
    }
}
