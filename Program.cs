using System;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Microsoft.Azure.Documents;
using System.Collections.Generic;
using Microsoft.Azure.Documents.Linq;
using System.Threading.Tasks;

namespace ChangeFeedSample
{
    class SampleDoc
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonConverter(typeof(IsoDateTimeConverter))]
        public DateTime UpdatedTime { get; set; }
    }
    class Program
    {
        private static DocumentClient client;

        private static readonly string DatabaseName = "test";
        private static readonly string CollectionName = "cfcoll";

        // Read the DocumentDB endpointUrl and authorizationKeys from config
        private static readonly string endpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string authorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];

        private static async Task RunSample()
        {
            using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey,
                    new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {

                client.CreateDatabaseIfNotExistsAsync(new Database { Id = DatabaseName }).Wait();

                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);

                // Delete it if exits
                try
                {
                    await client.DeleteDocumentCollectionAsync(collectionUri);
                }
                catch (Exception) { }
           

                DocumentCollection collectionDefinition = new DocumentCollection();
                collectionDefinition.Id = CollectionName;
                collectionDefinition.PartitionKey.Paths.Add("/id");

                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(DatabaseName),
                    collectionDefinition,
                    new RequestOptions { OfferThroughput = 2500 });


                //capture change feed run started time to use it as CF start time
                DateTime runStartedTime = DateTime.UtcNow;
                Console.WriteLine("Run started time {0}", runStartedTime);

                // wait for 2 secs
                Console.WriteLine("Waiting for 2 secs");
                System.Threading.Thread.Sleep(2000);


                var id1 = "Id1-" + System.Guid.NewGuid().ToString();
                var id2 = "Id2-" + System.Guid.NewGuid().ToString();

                Console.WriteLine("Inserting first docuemnt {0}",id1);
                // Insert 2 docs
                SampleDoc doc1 = new SampleDoc { Id = id1, UpdatedTime = DateTime.UtcNow };
                await client.CreateDocumentAsync(
                    collectionUri, doc1);

                // wait for 2 secs
                Console.WriteLine("Waiting for 2 secs");
                System.Threading.Thread.Sleep(2000);

                Console.WriteLine("Inserting second docuemnt {0}", id2);
                SampleDoc doc2 = new SampleDoc { Id = id2, UpdatedTime = DateTime.UtcNow };
                await client.CreateDocumentAsync(
                    collectionUri, doc2);

                // At this state doc1 timestamp < doc2 time stamp. The order is {doc1, doc2}
                //To prove that documents in each partition arranged in chronological order we are going to do the following:

                // Set the readCutOffTime to read only documents  which are updated <= readCutOffTime
                DateTime readCutOffTime = DateTime.UtcNow;
                Console.WriteLine("Read cutoff time {0}", readCutOffTime);

                // wait for 5 secs
                Console.WriteLine("Waiting for 5 secs");
                System.Threading.Thread.Sleep(5000);

                Console.WriteLine("Inserting a few more documents which won't processed in change feed" +
                    "since these are being added after read cutoff time...");

                for(int i=3; i<8;i++)
                {
                   var doc= new SampleDoc { Id = "Id"+i+"-"+System.Guid.NewGuid().ToString(), UpdatedTime = DateTime.UtcNow };
                    Console.WriteLine("Adding " + doc.Id);

                    await client.CreateDocumentAsync(
                   collectionUri, doc);
                }
                var doc1UpdateTime = DateTime.UtcNow;

                Console.WriteLine("updating doc {0} at time:{1}",id1, doc1UpdateTime);

                // Update the doc1
                var readDocResp = await client.ReadDocumentAsync(
                    UriFactory.CreateDocumentUri(DatabaseName, CollectionName, id1),
                    new RequestOptions { PartitionKey = new PartitionKey(id1) });
                var readDoc = (dynamic)readDocResp.Resource;
                readDoc.UpdatedTime = DateTime.UtcNow;
                await client.ReplaceDocumentAsync(readDoc);

                // Now internally order changes to {doc2, doc1} which is doc2 timestamp < doc1 timestamp
                // Put all the partition key ranges in a list
                List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();
                string pkRangesResponseContinuation = null;
                do
                {
                    var task = client.ReadPartitionKeyRangeFeedAsync(
                        collectionUri,
                        new FeedOptions { RequestContinuation = pkRangesResponseContinuation });
                    task.Wait();
                    FeedResponse<PartitionKeyRange> pkRangesResponse = task.Result;
                    partitionKeyRanges.AddRange(pkRangesResponse);
                    pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
                }
                while (pkRangesResponseContinuation != null);

                // Demo purpose, you have to store it into a place which can be recovered incase of process crash
                // This is not needed in some cases it depends on the business requirement
                Dictionary<string, string> checkpoints = new Dictionary<string, string>();

                // Now read all the documents from the startTime=RunstartedTime until readCutOffTime
                foreach (PartitionKeyRange pkRange in partitionKeyRanges)
                {
                    string continuation = null;
                    checkpoints.TryGetValue(pkRange.Id, out continuation);

                    IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                        collectionUri,
                        new ChangeFeedOptions
                        {
                            PartitionKeyRangeId = pkRange.Id,
                            StartFromBeginning = true,
                            RequestContinuation = continuation,
                            MaxItemCount = -1,
                            // Set reading time: only show change feed results modified since Run Started Time
                            StartTime = runStartedTime
                        });

                    // Stop reading from the partition if the document time stamp is greater than readCutOffTime
                    while (query.HasMoreResults)
                    {
                        FeedResponse<Document> readChangesResponse = query.ExecuteNextAsync<Document>().Result;

                        foreach (Document changedDocument in readChangesResponse)
                        {
                            // Stop processing the documents if the document updated after read Cut Off Time
                            if(changedDocument.Timestamp>readCutOffTime)
                            {
                                Console.WriteLine("Recieved a document last modified time : {0} > read cut off time: {1}, so stoping the process for partition: {2}",
                                    changedDocument.Timestamp,
                                    readCutOffTime,
                                    pkRange.Id);
                                break;
                            }

                            // Prints only documents if they are not modified before read cut off time
                            Console.WriteLine("Last modified time {0}", changedDocument.Timestamp);
                            Console.WriteLine("Read document {0} from the change feed.", changedDocument.Id);

                        }

                        checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                    }
                }

            }
        }
        static void Main(string[] args)
        {
            RunSample().Wait();
            Console.WriteLine("End of demo, press any key to exit.");
            Console.ReadKey();
        }
    }
}
