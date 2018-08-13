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
    public static class TestExtensions
    {
        public static IList<RegionTestData> CreateDataSetAndUploadIt(IList<RegionInputTestData> inputData)
        {
            var regionTasks = new List<Task<RegionTestData>>();

            foreach (var regionInputData in inputData)
            {
                // NOTE: it is not possible to use Parallel.ForEach because of long running tasks
                // https://social.msdn.microsoft.com/Forums/en-US/9432ed34-01de-4eac-8e6d-2beaa6c10528/long-running-task-in-parallelfor-and-parallelforeach?forum=parallelextensions
                var writeRegionToAzureTask = new Task<RegionTestData>(() => TestExtensions.CreateTestDataAndUploadItToAzure(regionInputData),
                                                                  TaskCreationOptions.LongRunning);
                writeRegionToAzureTask.Start();
                regionTasks.Add(writeRegionToAzureTask);
            }
            Task.WaitAll(regionTasks.ToArray());

            var regionData = regionTasks.Select(task => task.Result).ToList();

            return regionData;
        }

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
                AverageEntitySizeForQueriedEntitiesInKB = TestUtils.CalculateAverageEntitySize(peopleToQuery),
            };
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
    }
}
