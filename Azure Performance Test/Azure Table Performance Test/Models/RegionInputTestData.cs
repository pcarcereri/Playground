using System.Collections.Generic;

namespace AzureTablePerformanceTest
{
    public class RegionInputTestData
    {
        public string RegionName { get; internal set; }
        public int RegionPopulation { get; internal set; }
        public HashSet<int> PeopleToQueryIndexes { get; internal set; }
    }
}