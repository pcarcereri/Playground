using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace AzureTablePerformanceTest
{
    public static class Parameters
    {
        private static Dictionary<string, int> _italianRegionToPopulation;
        private static long _amountOfPeopleToUpload = 0;
        private static string _logPath;

        static Parameters()
        {
            string appSettingsPath = Directory.GetCurrentDirectory() + @"\..\..\..";
            var builder = new ConfigurationBuilder()
                            .SetBasePath(appSettingsPath)
                            .AddJsonFile("appsettings.json");

            IConfiguration Configuration = builder.Build();

            ConnectionString = Configuration["ConnectionString"];
            NumberOfPeopleToQueryPerRegion = int.Parse(Configuration["NumberOfPeopleToQueryPerRegion"]);
            TableName = Configuration["TableName"];
            LoadReductionFactor = int.Parse(Configuration["LoadReductionFactor"]);
            AzureBatchSize = int.Parse(Configuration["AzureBatchSize"]);

            InitializeItalianRegions();

            InitializeLogging();
        }

        public static int LoadReductionFactor { get; private set; }

        public static string ConnectionString { get; private set; }

        public static int NumberOfPeopleToQueryPerRegion { get; private set; }

        public static string TableName { get; private set; }

        public static int AzureBatchSize { get; private set; }

        public static string LogPath
        {
            get
            {
                return _logPath;
            }
        }

        public static IDictionary<string, int> ItalianRegionToPopulation
        {
            get
            {
                return _italianRegionToPopulation;
            }
        }

        public static long NumberOfPeopleToUpload
        {
            get
            {

                return _amountOfPeopleToUpload;
            }
        }

        private static void InitializeLogging()
        {
            _logPath = Path.Combine(Path.GetTempPath(), "Azure table test error log.txt");

            File.AppendAllText(_logPath, $"{Environment.NewLine}Start test at: {DateTime.Now}{Environment.NewLine}");
        }

        private static void InitializeItalianRegions()
        {
            // source: https://www.tuttitalia.it/regioni/popolazione/
            _italianRegionToPopulation = new Dictionary<string, int>()
            {
                { "Lombardia",   6036258 },
                { "Lazio",     4896693},
                { "Campania",      5826860},
                { "Sicilia",    5026989},
                { "Veneto",    4903722},
                { "Emilia Romagna",    5452629},
                { "Piemonte",      5375865},
                { "Puglia",   5048242},
                { "Toscana",    4736968},
                { "Calabria",      4956687},
                { "Sardegna",      1648176},
                { "Liguria",   1556981},
                { "Marche",    1531753},
                { "Abruzzo",    1315196},
                { "Friuli V. G.",     1216853},
                { "Trentino Alto Adige",    1067648},
                { "Umbria",    884640 },
                { "Basilicata",    567118},
                { "Molise",    308493 },
                { "V d'Aosta",      126202}
            };

            _italianRegionToPopulation = _italianRegionToPopulation
                .ToDictionary(kvp => kvp.Key, kvp => (int)kvp.Value / LoadReductionFactor);

            _italianRegionToPopulation.Values.ToList().ForEach(val => _amountOfPeopleToUpload += val);

        }
    }
}
