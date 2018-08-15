using Newtonsoft.Json;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Concurrent;

namespace AzureTablePerformanceTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("--- Azure Storage Table Performance Test ---");
            Console.WriteLine($"--- Uploading and querying a dataset of {Parameters.NumberOfPeopleToUpload.ToString("##,#")} people ---");
            Console.WriteLine($"--- People are partitioned into {Parameters.ItalianRegionToPopulation.Count} regions --- {Environment.NewLine}");

            AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionTrapper;

            AzureUtils.CreateTableIfNotExists(Parameters.TableName);

            IList<RegionInputTestData> testInputData = GenerateTestInputData();

            IList<RegionTestData> regionDataset = GenerateDatasetAndUploadItToAzure(testInputData);

            WriteDatasetToQueryOnFileIfRequired(regionDataset);

            PerformQueryOnRandomDataset(regionDataset);

            Console.ReadLine();
        }

        private static IList<RegionInputTestData> GenerateTestInputData()
        {
            var testInputData = new ConcurrentBag<RegionInputTestData>();

            Parallel.ForEach(Parameters.ItalianRegionToPopulation, (regionNameToPopulation) =>
             {
                 var region = new RegionInputTestData()
                 {
                     RegionName = regionNameToPopulation.Key,
                     RegionPopulation = regionNameToPopulation.Value,
                 };

                 HashSet<int> indexesOfRandomPeopleToQuery = ListUtils.GenerateListOfRandomIndexes(
                                                                         listLenght: Parameters.NumberOfPeopleToQueryPerRegion,
                                                                         maxIndex: region.RegionPopulation);
                 region.PeopleToQueryIndexes = indexesOfRandomPeopleToQuery;

                 testInputData.Add(region);
             });

            return testInputData.ToList();
        }

        private static IList<RegionTestData> GenerateDatasetAndUploadItToAzure(IList<RegionInputTestData> testInputData)
        {
            List<RegionTestData> outputData = new List<RegionTestData>();
            TimeSpan elapsedTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(() =>
                {
                    var regionOutputData = TestExtensions.CreateDataSetAndUploadIt(testInputData);
                    outputData.AddRange(regionOutputData);
                });

            RegionExecutionTime executionTime = TestUtils.CalculateEntitiesPerSecond(Parameters.NumberOfPeopleToUpload, elapsedTime);
            string averageEntitySize = TestUtils.GetTotalEntitySize(outputData);

            Console.WriteLine($"\nUploading {Parameters.NumberOfPeopleToUpload.ToString("##,#")} people to azure took: {executionTime.FormattedTime}");
            Console.WriteLine($"Average entity size {averageEntitySize} KB");
            Console.WriteLine($"Uploading average of {executionTime.EntitiesPerSecond} entities/s");

            return outputData;
        }

        private static void WriteDatasetToQueryOnFileIfRequired(IList<RegionTestData> regionData)
        {
            if (Parameters.WritePeopleToQueryToFile)
            {
                Parallel.ForEach(regionData, (region) =>
                {
                    string randomDatasetLocation = Path.Combine(Path.GetTempPath(), region.RegionName + "_random.json");
                    TestUtils.WritePeopleToFile(region.PeopleToQueryDataset, randomDatasetLocation);
                });
            }
        }

        private static void PerformQueryOnRandomDataset(IList<RegionTestData> regionData)
        {
            IList<Person> peopleToQuery = TestUtils.RetrieveRandomPeopleToQuery(regionData);

            var batchesToQuery = peopleToQuery.Batch(Parameters.AzureBatchSize);

            TimeSpan elapsedTime = TestUtils.MeasureExecutionTimeForOperationOnEntities(() =>
               {
                   Parallel.ForEach(batchesToQuery,
                                   (batchToQuery) => TestExtensions.QueryBatchOfPeople(batchToQuery));
               });

            RegionExecutionTime executionTime = TestUtils.CalculateEntitiesPerSecond(peopleToQuery.Count, elapsedTime);

            Console.WriteLine(Environment.NewLine);
            Console.WriteLine($"Retrieving {peopleToQuery.Count.ToString("##,#")} random people (c.a. {Parameters.NumberOfPeopleToQueryPerRegion} per region) took: {executionTime.FormattedTime}");
            Console.Write($"Querying average of {executionTime.EntitiesPerSecond} queries/s{Environment.NewLine}");
        }

        private static void UnhandledExceptionTrapper(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine($"Catched exception at {DateTime.Now}. See log for more details");
            File.AppendAllText(Parameters.LogPath, $"{DateTime.Now}: {e.ToString()}{Environment.NewLine}");
        }
    }
}
