using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;

namespace AzureTablePerformanceTest
{
    public static class ListUtils
    {
        private static Random _random = new Random();

        // https://stackoverflow.com/questions/13731796/create-batches-in-linq
        public static IEnumerable<IEnumerable<TSource>> Batch<TSource>(this IEnumerable<TSource> source, int size)
        {
            TSource[] bucket = null;
            var count = 0;

            foreach (var item in source)
            {
                if (bucket == null)
                    bucket = new TSource[size];

                bucket[count++] = item;
                if (count != size)
                    continue;

                yield return bucket;

                bucket = null;
                count = 0;
            }

            if (bucket != null && count > 0)
                yield return bucket.Take(count);
        }

        public static HashSet<int> GenerateListOfRandomIndexes(int listLenght, int maxIndex)
        {
            HashSet<int> randomIndexes = new HashSet<int>();

            // TODO: improve this algorithm
            bool canGenerateRandomIndexes = maxIndex > listLenght;

            int maxNumberOfTrialsBeforeContinuingTheGeneration = 10;

            for (int i = 0; i < listLenght; i++)
            {
                int randomIndex = i;

                if (canGenerateRandomIndexes)
                {
                    randomIndex = _random.Next(maxIndex);

                    int trials = 0;
                    while (randomIndexes.Contains(randomIndex))
                    {
                        randomIndex = _random.Next(maxIndex);
                        trials++;
                        if (trials > maxNumberOfTrialsBeforeContinuingTheGeneration)
                        {
                            // trying 10 times, them breaking the loop
                            break;
                        }
                    }
                }
                randomIndexes.Add(randomIndex);
            }
            return randomIndexes;
        }

        public static IEnumerable<TSource> GetRandomDataset<TSource>(this IEnumerable<TSource> source, int size)
        {
            for (int i = 0; i < size; i++)
            {
                int randomIndex = _random.Next(source.Count());
                yield return source.ElementAt(randomIndex);
            }
        }

        //https://stackoverflow.com/questions/273313/randomize-a-listt
        public static void Shuffle<T>(this IList<T> list)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = _random.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }
}
