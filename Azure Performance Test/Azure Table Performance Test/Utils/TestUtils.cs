using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTablePerformanceTest
{
    public static class TestUtils
    {
        private static Random _random = new Random();

        // https://stackoverflow.com/questions/1344221/how-can-i-generate-random-alphanumeric-strings-in-c
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[_random.Next(s.Length)]).ToArray());
        }

        public static void WritePeopleToFile(IEnumerable<Person> people, string fileName)
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }

            // write batches in order to avoid out of memory exceptions
            int batchSize = 100000;
            var batches = people.Batch(batchSize);
            foreach (var batch in batches)
            {
                string serializedJsonPeople = JsonConvert.SerializeObject(batch);

                File.AppendAllText(fileName, serializedJsonPeople);
            }
        }

        public static IEnumerable<Person> ReadPeopleFromFile(string fileName)
        {
            // using stream for better performances
            //https://www.newtonsoft.com/json/help/html/performance.htm
            using (Stream s = File.OpenRead(fileName))
            using (StreamReader sr = new StreamReader(s))
            using (JsonReader reader = new JsonTextReader(sr))
            {
                JsonSerializer serializer = new JsonSerializer();

                // read the json from a stream
                // json size doesn't matter because only a small piece is read at a time from the HTTP request
                return serializer.Deserialize<IEnumerable<Person>>(reader);
            }
        }

        public static RegionData CreateTestDataAndUploadItToAzure(string regionName, int regionPopulation)
        {
            List<Person> peopleBatch = new List<Person>();
            List<Person> peopleToQueryDataset = new List<Person>();
            int sizeOfPeopleToQuery = (regionPopulation / 100) * Parameters.PercentageOfPeopleToQuery;

            int batchSize = 100;
            for (int i = 1; i <= regionPopulation; i++)
            {
                peopleBatch.Add(new Person(regionName));

                if (i % batchSize == 0 || i == regionPopulation)
                {
                    InsertBatchIntoTable(peopleBatch);
                    peopleToQueryDataset.AddRange(peopleBatch.GetRandomDataset(size: sizeOfPeopleToQuery));
                    peopleBatch.Clear();
                }
            }

            Console.WriteLine($"Uploaded {regionPopulation} people for region: '{regionName}'");

            return new RegionData()
            {
                RegionName = regionName,
                RegionPopulation = regionPopulation,
                PeopleToQueryDataset = peopleToQueryDataset,
                AverageEntitySizeForQueriedEntitiesInKB = CalculateAverageEntitySize(peopleToQueryDataset),
            };
        }

        public static string GetTotalEntitySize(List<RegionData> regionData)
        {
            double regionTotalSize = 0;
            regionData.ForEach(region => regionTotalSize += region.AverageEntitySizeForQueriedEntitiesInKB);
            double averageEntitySize = regionTotalSize / regionData.Count;
            return averageEntitySize.ToString("f2");
        }

        public static long GetTotalRegionPopulation(List<RegionData> regionData)
        {
            long amountOfPeopleToUpload = 0;
            regionData.ForEach(region => amountOfPeopleToUpload += region.RegionPopulation);
            return amountOfPeopleToUpload;
        }


        private static double CalculateAverageEntitySize(IEnumerable<Person> peopleDataset)
        {
            long totalSizeInByte = 0;

            foreach (var person in peopleDataset)
            {
                // https://blogs.msdn.microsoft.com/avkashchauhan/2011/11/30/how-the-size-of-an-entity-is-caclulated-in-windows-azure-table-storage/
                //4 bytes + Len(PartitionKey + RowKey) * 2 bytes + For - Each Property(8 bytes + Len(Property Name) * 2 bytes + Sizeof(.Net Property Type))
                int personSize = 4 + (person.PartitionKey.Length + person.RowKey.Length) * 2 +
                                    8 + "FirstName".Length * 2 + person.FirstName.Length * 2 +
                                    8 + "LastName".Length * 2 + person.LastName.Length * 2 +
                                    8 + "TaxNumber".Length * 2 + person.TaxNumber.Length * 2 +
                                    8 + "Age".Length * 2 + 4;

                totalSizeInByte += personSize;

            }

            double totalSizeInByteInKB = totalSizeInByte / 1000;
            double averageEntitySizeForRegion = totalSizeInByteInKB / peopleDataset.Count();
            return averageEntitySizeForRegion;
        }

        internal static void QueryPeopleBatch(IEnumerable<Person> batch)
        {
            var tasks = new List<Task>();
            CloudTable cloudTable = AzureUtils.GetCloudTable(Parameters.TableName);

            foreach (var person in batch)
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<Person>(person.PartitionKey, person.RowKey);

                TableResult retrievedResult = cloudTable.ExecuteAsync(retrieveOperation).Result;

                if (retrievedResult.Result == null)
                {
                    throw new InvalidOperationException($"Cannot retrieve entity. Partition key: {person.PartitionKey}, Row key: {person.RowKey}");
                }
            }
        }

        public static void InsertBatchIntoTable(IEnumerable<Person> people)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();
            CloudTable cloudTable = AzureUtils.GetCloudTable(Parameters.TableName);

            foreach (var person in people)
            {
                batchOperation.Insert(person);
            }

            cloudTable.ExecuteBatchAsync(batchOperation).Wait();
        }

        public static RegionExecutionTime MeasureExecutionTimeForOperationOnEntities(Action action, long numberOfPeople)
        {
            Stopwatch timer = Stopwatch.StartNew();

            action();

            timer.Stop();

            TimeSpan executionTime = timer.Elapsed;

            string executionTimeString = String.Format("{0:00}hrs {1:00}min {2:00}s", executionTime.Hours, executionTime.Minutes, executionTime.Seconds);

            // we use milliseconds in case the operation took less than 1 second 
            double elapsedSeconds = executionTime.Seconds + ((double)executionTime.Milliseconds / 1000);
            double entitiesPerSecond = (double)numberOfPeople / elapsedSeconds;
            string entitiesPerSecondString = entitiesPerSecond.ToString("f2");

            return new RegionExecutionTime() { FormattedTime = executionTimeString, EntitiesPerSecond = entitiesPerSecondString };
        }
    }
}
