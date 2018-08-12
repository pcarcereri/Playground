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

        public static RegionTestData CreateTestDataAndUploadItToAzure(RegionInputTestData regionInputData)
        {
            List<Person> peopleBatch = new List<Person>();
            List<Person> peopleToQuery = new List<Person>();

            for (int counter = 1; counter <= regionInputData.RegionPopulation; counter++)
            {
                var newPerson = new Person(regionInputData.RegionName);
                peopleBatch.Add(newPerson);

                if (regionInputData.PeopleToQueryIndexes.Contains(counter))
                {
                    peopleToQuery.Add(newPerson);
                }

                if (counter % Parameters.AzureBatchSize == 0 || counter == regionInputData.RegionPopulation)
                {
                    InsertBatchIntoTable(peopleBatch);
                    peopleBatch.Clear();
                }
            }

            Console.WriteLine($"Uploaded {regionInputData.RegionPopulation.ToString("##,#")} people for region: '{regionInputData.RegionName}'");

            return new RegionTestData()
            {
                RegionName = regionInputData.RegionName,
                RegionPopulation = regionInputData.RegionPopulation,
                PeopleToQueryDataset = peopleToQuery,
                AverageEntitySizeForQueriedEntitiesInKB = CalculateAverageEntitySize(peopleToQuery),
            };
        }

        public static string GetTotalEntitySize(List<RegionTestData> regionData)
        {
            double regionTotalSize = 0;
            regionData.ForEach(region => regionTotalSize += region.AverageEntitySizeForQueriedEntitiesInKB);
            double averageEntitySize = regionTotalSize / regionData.Count;
            return averageEntitySize.ToString("f2");
        }

        public static long GetTotalRegionPopulation(List<RegionTestData> regionData)
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

        public static List<Person> RetrieveRandomPeopleToQuery(List<RegionTestData> regionData)
        {
            List<Person> peopleToQuery = regionData
                            .SelectMany(regionInfo => regionInfo.PeopleToQueryDataset)
                            .ToList();

            peopleToQuery.Shuffle();

            return peopleToQuery;
        }

        public static void QueryBatchOfPeople(IEnumerable<Person> batchToQuery)
        {
            CloudTable cloudTable = AzureUtils.GetCloudTable(Parameters.TableName);

            Parallel.ForEach(batchToQuery, (person) =>
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<Person>(person.PartitionKey, person.RowKey);

                var queryTask = cloudTable.ExecuteAsync(retrieveOperation);
            });
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

        public static RegionExecutionTime MeasureExecutionTimeForOperationOnEntities(long numberOfPeople, Action action)
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

            return new RegionExecutionTime()
            {
                FormattedTime = executionTimeString,
                EntitiesPerSecond = entitiesPerSecondString
            };
        }

        //https://stackoverflow.com/questions/4470700/how-to-save-console-writeline-output-to-text-file
        public static void RedirectConsoleToLogFile()
        {
            string logPath = Parameters.LogPath;
            if (File.Exists(logPath))
            {
                File.Delete(logPath);
            }

            // stream should be closed at the end of the program 
            FileStream ostrm = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.Write);
            StreamWriter writer = new StreamWriter(ostrm);
            Console.SetOut(writer);
        }

        // https://stackoverflow.com/questions/1344221/how-can-i-generate-random-alphanumeric-strings-in-c
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[_random.Next(s.Length)]).ToArray());
        }

        public static void WritePeopleToFile(IEnumerable<Person> people, string fileName)
        {
            string serializedJsonPeople = JsonConvert.SerializeObject(people);

            File.WriteAllText(fileName, serializedJsonPeople);
        }

        public static IEnumerable<Person> ReadPeopleToFile(string fileName)
        {
            string jsonPeople = File.ReadAllText(fileName);

            return JsonConvert.DeserializeObject<IEnumerable<Person>>(jsonPeople);
        }
    }
}
