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

        public static long GetTotalRegionPopulation(List<RegionTestData> regionData)
        {
            long amountOfPeopleToUpload = 0;
            regionData.ForEach(region => amountOfPeopleToUpload += region.RegionPopulation);
            return amountOfPeopleToUpload;
        }

        public static string GetTotalEntitySize(List<RegionTestData> regionData)
        {
            double regionTotalSizeInByte = 0;
            regionData.ForEach(region => regionTotalSizeInByte += region.AverageEntitySizeForQueriedEntitiesInByte);
            double averageEntitySizeInByte = regionTotalSizeInByte / regionData.Count;
            double averageEntitySizeInKB = regionTotalSizeInByte / 1000;
            return averageEntitySizeInKB.ToString();
        }

        public static double CalculateAverageEntitySize(List<Person> peopleDataset)
        {
            long totalSizeInByte = 0;

            foreach (var person in peopleDataset)
            {
                totalSizeInByte += GetEntitySizeInByte(person);
            }

            double averageEntitySizeForRegion = totalSizeInByte / peopleDataset.Count;
            return averageEntitySizeForRegion;
        }

        public static int GetEntitySizeInByte(Person person)
        {
            // https://blogs.msdn.microsoft.com/avkashchauhan/2011/11/30/how-the-size-of-an-entity-is-caclulated-in-windows-azure-table-storage/
            //4 bytes + Len(PartitionKey + RowKey) * 2 bytes + For - Each Property(8 bytes + Len(Property Name) * 2 bytes + Sizeof(.Net Property Type))
            return 4 + (person.PartitionKey.Length + person.RowKey.Length) * 2 +
                                8 + "FirstName".Length * 2 + person.FirstName.Length * 2 +
                                8 + "LastName".Length * 2 + person.LastName.Length * 2 +
                                8 + "TaxNumber".Length * 2 + person.TaxNumber.Length * 2 +
                                8 + "Age".Length * 2 + 4;
        }

        public static IList<Person> RetrieveRandomPeopleToQuery(IList<RegionTestData> regionData)
        {
            List<Person> peopleToQuery = regionData
                            .SelectMany(regionInfo => regionInfo.PeopleToQueryDataset)
                            .ToList();

            peopleToQuery.Shuffle();

            return peopleToQuery;
        }

        public static TimeSpan MeasureExecutionTimeForOperationOnEntities(Action action)
        {
            Stopwatch timer = Stopwatch.StartNew();

            action();

            timer.Stop();

            TimeSpan executionTime = timer.Elapsed;

            return executionTime;
        }

        public static RegionExecutionTime CalculateEntitiesPerSecond(long numberOfPeople, TimeSpan executionTime)
        {
            string executionTimeString = String.Format("{0:00}hrs {1:00}min {2:00}s", executionTime.Hours, executionTime.Minutes, executionTime.Seconds);

            // we use milliseconds in case the operation took less than 1 second 
            double elapsedSeconds = executionTime.TotalSeconds + ((double)executionTime.Milliseconds / 1000);
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
