using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models;
using AAMVA.Core.Logging;
using AAMVA.Core.TPSFramework.PerformanceAttributes.Counters;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors
{
    public class DynamoDbReadLoadActor : AwsLoadTestActorBase, IDynamoDbReadActor
    {
        private AmazonDynamoDBClient _amazonDynamoDbClient;
        private DynamoDBContext _dynamoDbContext;
        private readonly int _itemsToTake;
        private readonly bool _getRealCount;

        public DynamoDbReadLoadActor()
        {
            string recordsToFetch = ConfigurationManager.AppSettings["NumberofRecordsToFetch"];
            Logger.LogInfo($"Number of records to fetch = {recordsToFetch}");

            string getRealCount = ConfigurationManager.AppSettings["GetRealCount"];
            Logger.LogInfo($"Do real count = {getRealCount}");

            _itemsToTake = string.IsNullOrEmpty(recordsToFetch) ? 20 : Convert.ToInt16(recordsToFetch);
            _getRealCount = !string.IsNullOrEmpty(getRealCount);
        }

        
        //to be used with TransactionLog table where partial MsgLocator is the sort key
        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb Read Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb Read Duration")]
        public List<T1> Read<T1, T2, T3>(string transOrigin, T2 messageProp, out int count) where T1 : class  
                                                                                            where T2: IMessageProp
        {
            IEnumerable<T1> results;
            Logger.LogInfo($"Called with queryoperator with sortkey start value {messageProp.GetPrimarySortKeyPrefix<T3>()} and end value {messageProp.GetSortKeyRangeEnd<T3>()}");
          
            results = !string.IsNullOrEmpty(messageProp.SortKeyRangeEnd) ?
                        _dynamoDbContext.Query<T1>(transOrigin, QueryOperator.Between, messageProp.GetPrimarySortKeyPrefix<T3>(), messageProp.GetSortKeyRangeEnd<T3>()) :
                        _dynamoDbContext.Query<T1>(transOrigin, QueryOperator.BeginsWith, messageProp.GetPrimarySortKeyPrefix<T3>());

            count = _getRealCount ? results.Count() : _itemsToTake;

            var result1 = results.Take(_itemsToTake).ToList();

            Logger.LogInfo($"Read result returned {count}");

            return result1;
        }

        

        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadGsi Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadGsi Duration")]
        public List<T1> ReadGsi<T1>(string appId, string messageOrigin, out int count) where T1 : class 
        {
            var results = _dynamoDbContext.Query<T1>(appId, QueryOperator.Equal, new[] {messageOrigin},
                new DynamoDBOperationConfig
                {
                    IndexName = "AppID-MsgOrigin-index"
                });
           
            var result1 = results.Take(_itemsToTake).ToList();
            count = result1.Count();
            return result1;
        }

        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadLsi Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadLsi Duration")]
        public List<T1> ReadLsi<T1>(string transOrigin, string messageType, out int count) where T1 : class
        {
            var results = _dynamoDbContext.Query<T1>(transOrigin, QueryOperator.Equal, new[] {messageType},
                new DynamoDBOperationConfig
                {
                    IndexName = "TransOrigin-MsgType-index"
                });
            var result1 = results.Take(_itemsToTake).ToList();
            count = result1.Count();
            return result1;
        }

        [InstrumentInvocationsPerSecond("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadLsi2 Per Second")]
        [InstrumentAverageTime("Aamva.Ncs.LoadTestFramework.DynamoDbActors", "DynamoDb ReadLsi2 Duration")]
        public List<T1> ReadLsi2<T1>(string transOrigin, string messageDest, out int count) where T1 : class
        {
            var results = _dynamoDbContext.Query<T1>(transOrigin, QueryOperator.Equal, new[] {messageDest},
                new DynamoDBOperationConfig
                {
                    IndexName = "TransOrigin-MsgDestination-index"
                });
            var result1 = results.Take(_itemsToTake).ToList();
            count = result1.Count();
            return result1;
        }

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
    }
}
