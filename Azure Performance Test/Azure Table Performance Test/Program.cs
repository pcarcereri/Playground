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
            Console.WriteLine($"--- Loading {Parameters.NumberOfPeopleToUpload.ToString("##,#")} people splitted into {Parameters.ItalianRegionToPopulation.Count} regions ---\n");

            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionTrapper;

            AzureUtils.CreateTableIfNotExists(Parameters.TableName);

            var regionInputData = GenerateTestInputData();

            var regionOutputData = GenerateDatasetAndUploadItToAzure(regionInputData);

            QueryRandomDataset(regionOutputData);

            Console.ReadLine();
        }

        private static List<RegionInputTestData> GenerateTestInputData()
        {
            var regionData = new List<RegionInputTestData>();

            foreach (var regionNameToPopulation in Parameters.ItalianRegionToPopulation)
            {
                var region = new RegionInputTestData()
                {
                    RegionName = regionNameToPopulation.Key,
                    RegionPopulation = regionNameToPopulation.Value,
                };

                HashSet<int> indexesOfRandomPeopleToQuery = ListUtils.GenerateListOfRandomIndexes(
                                                                        length: Parameters.NumberOfPeopleToQueryPerRegion,
                                                                        maxIndex: region.RegionPopulation);

                region.PeopleToQueryIndexes = indexesOfRandomPeopleToQuery;
                regionData.Add(region);
            }

            return regionData;
        }

        private static List<RegionTestData> GenerateDatasetAndUploadItToAzure(List<RegionInputTestData> testInputData)
        {
            List<RegionTestData> outputData = new List<RegionTestData>();
            RegionExecutionTime executionTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(Parameters.NumberOfPeopleToUpload,
                () =>
                {
                    var regionOutputData = CreateDataSetAndUploadIt(testInputData);
                    outputData.AddRange(regionOutputData);
                });

            string averageEntitySize = TestUtils.GetTotalEntitySize(outputData);

            Console.WriteLine($"\nUploading {Parameters.NumberOfPeopleToUpload.ToString("##,#")} people to azure took: {executionTime.FormattedTime}");
            Console.WriteLine($"Average entity size {averageEntitySize} KB");
            Console.WriteLine($"Uploading average of {executionTime.EntitiesPerSecond} entities/s");

            return outputData;
        }

        private static List<RegionTestData> CreateDataSetAndUploadIt(List<RegionInputTestData> inputData)
        {
            var regionTasks = new List<Task<RegionTestData>>();

            foreach (var regionInputData in inputData)
            {
                // NOTE: it is not possible to use Parallel.ForEach because of long running tasks
                // https://social.msdn.microsoft.com/Forums/en-US/9432ed34-01de-4eac-8e6d-2beaa6c10528/long-running-task-in-parallelfor-and-parallelforeach?forum=parallelextensions
                var writeRegionToAzureTask = new Task<RegionTestData>(() => TestUtils.CreateTestDataAndUploadItToAzure(regionInputData),
                                                                  TaskCreationOptions.LongRunning);
                writeRegionToAzureTask.Start();
                regionTasks.Add(writeRegionToAzureTask);
            }
            Task.WaitAll(regionTasks.ToArray());

            var regionData = regionTasks.Select(task => task.Result).ToList();

            regionData.ForEach(data =>
            {
                string randomDatasetLocation = Path.Combine(Path.GetTempPath(), data.RegionName + "_random.json");
                TestUtils.WritePeopleToFile(data.PeopleToQueryDataset, randomDatasetLocation);
            });
            return regionData;
        }

        private static void QueryRandomDataset(List<RegionTestData> regionData)
        {
            List<Person> peopleToQuery = TestUtils.RetrieveRandomPeopleToQuery(regionData);

            var batchesToQuery = peopleToQuery.Batch(Parameters.AzureBatchSize);

            RegionExecutionTime executionTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(peopleToQuery.Count,
                () =>
                {
                    Parallel.ForEach(batchesToQuery, (batchToQuery) => TestUtils.QueryBatchOfPeople(batchToQuery));
                });

            Console.WriteLine(Environment.NewLine);
            Console.WriteLine($"Retrieved {peopleToQuery.Count.ToString("##,#")} random people (max. {Parameters.NumberOfPeopleToQueryPerRegion} people per region) took: {executionTime.FormattedTime}");
            Console.Write($"Querying average of {executionTime.EntitiesPerSecond} queries/s{Environment.NewLine}");
        }

        private static void UnhandledExceptionTrapper(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine($"Catched exception at {DateTime.Now}. See log for more details");
            File.AppendAllText(Parameters.LogPath, $"{DateTime.Now}: {e.ToString()}{Environment.NewLine}");
        }
    }
}
