using System.Collections.Generic;

namespace AzureTablePerformanceTest
{
    public class RegionData
    {
        public string RegionName { get; set; }
        public int RegionPopulation { get; set; }
        public IEnumerable<Person> PeopleDataset { get; set; }
        public IEnumerable<Person> PeopleToQueryDataset { get; set; }
        public double AverageEntitySizeForQueriedEntitiesInKB { get; set; }
    }
}