using System;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Helpers;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;

using AAMVA.Core.Logging;
using AAMVA.Core.TPSFramework.PerformanceAttributes.Counters;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors
{
    public class DynamoDbWriteLoadActor : AwsLoadTestActorBase, IDynamoDbWriteActor
    {
        private AmazonDynamoDBClient _amazonDynamoDbClient;
        private DynamoDBContext _dynamoDbContext;
        
        public override void Initialize()
        {
            //client and context
            _amazonDynamoDbClient = SetupDynamo();
            _dynamoDbContext = GetContext(_amazonDynamoDbClient);
        }

        private AmazonDynamoDBClient SetupDynamo()
        {
            var client =
                new AmazonDynamoDBClient(GetAwsCredentials(), SetClientConfigProperties(new AmazonDynamoDBConfig()));

            return client;
        }

        private DynamoDBContext GetContext(AmazonDynamoDBClient client)
        {
            DynamoDBContext dbContext = new DynamoDBContext(client);
            return dbContext;
        }

        protected override void Close()
        {
            base.Close();
            _dynamoDbContext?.Dispose();
            _amazonDynamoDbClient?.Dispose();
        }

        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb Write Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb Write Duration")]
  
        public void Write<T1>(T1 log)
        { 

            try
            {
               
                _dynamoDbContext.Save(log);
            }
            catch (Exception e)
            {
                Logger.LogError($"exception if Save: {e}");           
            }
        }

        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb WriteAsync Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb WriteAsync Duration")]
        public async Task WriteAsync<T1>(T1 log) 
        {
            try
            {
              
               await _dynamoDbContext.SaveAsync(log).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.LogError($"exception if SaveAsync: {e}");
            }
        }
    }
}
