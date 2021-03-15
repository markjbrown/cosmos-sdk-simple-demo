using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;

namespace sample
{
    class Program
    {
        private static readonly string uri = "https://localhost:8081";
        private static readonly string key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        private static readonly string databaseId = "database1";
        private static readonly string containerId = "families";

        private static Container container;
        private static CosmosClient client;


        static async Task Main(string[] args)
        {
            client = new CosmosClient(uri, key);
            Database db = await client.CreateDatabaseIfNotExistsAsync(databaseId);
            container = await db.CreateContainerIfNotExistsAsync(containerId, "/lastName", 400);

            Family family;

            family = await Create();

            await Query(family.LastName, family.Id);

            await Get(family.LastName, family.Id);

            family.Age = 40;

            await Update(family);

            await Transaction();

            await Delete(family.LastName, family.Id);
            
        }

        public static async Task<Family> Create()
        {
            Family family = new Family
            {
                Id = Guid.NewGuid().ToString(),
                FirstName = "Mark",
                LastName = "Brown",
                Age = 30
            };

            Response<Family> response = await container.CreateItemAsync<Family>(
                family,
                new PartitionKey(family.LastName));

            Print(response.Resource);

            Console.WriteLine(response.RequestCharge);

            return response.Resource;
        }

        public static async Task<List<Family>> Query(string lastName, string id)
        {
            List<Family> families = new List<Family>();

            string sql = "select * from c where c.id = @id";

            FeedIterator<Family> resultSet = container.GetItemQueryIterator<Family>(
                new QueryDefinition(sql)
                .WithParameter("@id", id),
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = new PartitionKey(lastName)
                });

            while (resultSet.HasMoreResults)
            {
                FeedResponse<Family> response = await resultSet.ReadNextAsync();

                foreach (Family family in response)
                {
                    families.Add(family);
                }

                Console.WriteLine(response.RequestCharge);
            }
            return families;
        }

        public static async Task Get(string lastName, string id)
        {
            Response<Family> response = await container.ReadItemAsync<Family>(
                id: id,
                partitionKey: new PartitionKey(lastName));

            Console.WriteLine(response.RequestCharge);

        }

        public static async Task Update(Family family)
        {
            
            await container.ReplaceItemAsync<Family>(
                family,
                id: family.Id,
                partitionKey: new PartitionKey(family.LastName),
                new ItemRequestOptions
                {
                    //IfMatchEtag = response.ETag
                });
        }

        public static async Task Delete(string lastName, string id)
        {
            await container.DeleteItemAsync<Family>(id, new PartitionKey(lastName));
        }

        public static async Task Transaction()
        {
            Family family1 = new Family
            {
                Id = Guid.NewGuid().ToString(),
                FirstName = "David",
                LastName = "Brown",
                Age = 25
            };

            Family family2 = new Family
            {
                Id = Guid.NewGuid().ToString(),
                FirstName = "Mary",
                LastName = "Brown",
                Age = 27
            };

            TransactionalBatchResponse batch = await container.CreateTransactionalBatch(
                new PartitionKey("Brown"))
                .CreateItem<Family>(family1)
                .CreateItem<Family>(family2)
                .ExecuteAsync();

            Console.WriteLine(batch.RequestCharge);

        }

        public static void Print(object obj)
        {
            Console.WriteLine($"{JObject.FromObject(obj).ToString()}\n");
        }
    }
}
