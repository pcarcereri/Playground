using System;
using System.Linq;
using System.Collections.Generic;

namespace AzureTablePerformanceTest
{
    public static class Parameters
    {
        private static Dictionary<string, int> _italianRegionToPopulation;
        private static long _amountOfPeopleToUpload = 0;

        static Parameters()
        {
            InitializeItalianRegions();
        }

        public static int PercentageOfPeopleToQuery { get; private set; } = 1;

        public static string TableName { get; private set; } = "ItaliansRegion";

        public static IDictionary<string, int> ItalianRegionToPopulation
        {
            get
            {
                return _italianRegionToPopulation;
            }
        }

        public static long AmountOfPeopleToUpload
        {
            get
            {

                return _amountOfPeopleToUpload;
            }
        }

        private static void InitializeItalianRegions()
        {
            // source: https://www.tuttitalia.it/regioni/popolazione/
            _italianRegionToPopulation = new Dictionary<string, int>()
            {
                { "Lombardia",   10036258 },
                { "Lazio",     5896693},
                { "Campania",      5826860},
                { "Sicilia",    5026989},
                { "Veneto",    4903722},
                { "Emilia Romagna",    4452629},
                { "Piemonte",      4375865},
                { "Puglia",   4048242},
                { "Toscana",    3736968},
                { "Calabria",      1956687},
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

            _italianRegionToPopulation.Values.ToList().ForEach(val => _amountOfPeopleToUpload += val);
        }
    }
}
