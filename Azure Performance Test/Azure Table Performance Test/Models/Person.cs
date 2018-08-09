using Microsoft.WindowsAzure.Storage.Table;
using RandomNameGeneratorLibrary;
using System;

namespace AzureTablePerformanceTest
{
    public class Person : TableEntity
    {
        public Person(string region)
        {
            this.PartitionKey = region;
            this.RowKey = Guid.NewGuid().ToString();

            var personGenerator = new PersonNameGenerator();

            FirstName = personGenerator.GenerateRandomFirstName();
            LastName = personGenerator.GenerateRandomLastName();
            Age = new Random().Next(1, 100);
            Address = TestUtils.RandomString(100);
            TaxNumber = Guid.NewGuid().ToString();
        }

        public Person() { }

        public string FirstName { get; set; }

        public string LastName { get; set; }

        public string Address { get; set; }

        public int Age { get; set; }

        public string TaxNumber { get; set; }
    }
}