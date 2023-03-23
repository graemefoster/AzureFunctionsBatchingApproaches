using System.Collections.Generic;
using System.Linq;

namespace BatchDurable;

public static class EnumerableEx
{
    
    public static IEnumerable<IEnumerable<T>> BreakBatch<T>(this IEnumerable<T> batch, int size)
    {
        var enumerator = batch.GetEnumerator();
        var innerBatch = new List<T>();
        while (enumerator.MoveNext())
        {
            innerBatch.Add(enumerator.Current);
            if (innerBatch.Count == size)
            {
                yield return innerBatch;
                innerBatch = new List<T>();
            }
        }

        if (innerBatch.Any())
        {
            yield return innerBatch;
        }
    }
}