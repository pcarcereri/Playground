using Newtonsoft.Json;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("--- Azure Storage Table Performance Test ---");
            Console.WriteLine($"--- Loading {Parameters.AmountOfPeopleToUpload} people splitted into {Parameters.ItalianRegionToPopulation.Count} regions ---\n");

            AzureUtils.CreateTableIfNotExists(Parameters.TableName);

            var regionData = GenerateDatasetAndUploadItToAzure(reuseExistingData: false);

            QueryRandomDataset(regionData);

            Console.ReadLine();
        }

        private static List<RegionData> GenerateDatasetAndUploadItToAzure(bool reuseExistingData)
        {
            var regionTasks = new List<Task<RegionData>>();

            RegionExecutionTime executionTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(() =>
            {
                foreach (var regionNameToPopulation in Parameters.ItalianRegionToPopulation)
                {
                    var writeRegionToAzureTask = new Task<RegionData>(() =>
                                                           TestUtils.CreateTestDataAndUploadItToAzure(regionName: regionNameToPopulation.Key,
                                                                                                      regionPopulation: regionNameToPopulation.Value),
                                                           TaskCreationOptions.LongRunning);
                    writeRegionToAzureTask.Start();
                    regionTasks.Add(writeRegionToAzureTask);
                }
                Task.WaitAll(regionTasks.ToArray());

            }, numberOfPeople: Parameters.AmountOfPeopleToUpload);

            var regionData = regionTasks.Select(task => task.Result).ToList();
            string averageEntitySize = TestUtils.GetTotalEntitySize(regionData);

            Console.WriteLine($"\nUploading {Parameters.AmountOfPeopleToUpload} people to azure took: {executionTime.FormattedTime}");
            Console.WriteLine($"Average entity size {averageEntitySize} KB");
            Console.WriteLine($"Uploading average of {executionTime.EntitiesPerSecond} entities/s");

            return regionData;
        }

        private static void QueryRandomDataset(List<RegionData> regionData)
        {
            List<Person> peopleToQuery = regionData
                .SelectMany(regionInfo => regionInfo.PeopleToQueryDataset)
                .ToList();

            peopleToQuery.Shuffle();

            int queryBatchSize = 100;
            var batches = peopleToQuery.Batch(queryBatchSize);

            var tasks = new List<Task>();
            RegionExecutionTime executionTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(() =>
            {
                foreach (var batch in batches)
                {
                    var queryPeopleTask = Task.Run(() => TestUtils.QueryPeopleBatch(batch));
                    tasks.Add(queryPeopleTask);
                }
                Task.WaitAll(tasks.ToArray());
            }, numberOfPeople: peopleToQuery.Count);

            Console.WriteLine($"\nQueried {peopleToQuery.Count} random people ({Parameters.PercentageOfPeopleToQuery}% of the total)");
            Console.Write($"Querying average of {executionTime.EntitiesPerSecond} queries/s");
        }
    }
}
