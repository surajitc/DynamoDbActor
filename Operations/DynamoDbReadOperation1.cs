using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Aamva.Ncs.LoadTestFramework.Core;
using Aamva.Ncs.LoadTestFramework.Core.Interfaces;
using Aamva.Ncs.LoadTestFramework.Core.Operations;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models;
using AAMVA.Core.Logging;


namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Operations
{
    

    public class DynamoDbRead1Operation : ParallelOperationBase
    {
        private RoundRobinQueue<MessageProp1> _messageProp1;
        private readonly RoundRobinQueue<int> _operationNumbers;

        private readonly Random _randomProps;
        private readonly Random _randomOps;

        private IDynamoDbReadActor _dynamoDbReadActor;

        public DynamoDbRead1Operation()
        {
            InitializeMessageProps();
            _operationNumbers = new RoundRobinQueue<int>(new List<int>() {1, 2, 3, 4});
            
            //_randomProps = new Random();
            //_randomOps = new Random();
        }

        protected override void InitializeOperation(ILoadTestActor actor)
        {
            _dynamoDbReadActor = (IDynamoDbReadActor)actor;
        }

        protected override async Task ActAsync()
        {
            var messageProp = _messageProp1.GetNextItem(); 
            var opNumber = ConfigurationManager.AppSettings["OpNumber"];
            int operationNumber = string.IsNullOrEmpty(opNumber) ? _operationNumbers.GetNextItem() : Convert.ToInt16(opNumber);
            int count;
          
            await Task.Run(() =>
            {
                switch (operationNumber)
                {
                    case 1:
                        Logger.LogInfo($"calling Read {messageProp.TransOrigin}-{messageProp.GetPrimarySortKeyPrefix<string>()}");
                        var results1 = _dynamoDbReadActor.Read<TransactionLog, IMessageProp, string>(messageProp.TransOrigin, messageProp, out count);
                        Logger.LogInfo($"Read {messageProp.TransOrigin}-{messageProp.GetPrimarySortKeyPrefix<string>()} result count {count}");
                        break;

                    case 2:
                        var results2 = _dynamoDbReadActor.ReadGsi<TransactionLog>(messageProp.AppId, messageProp.MessageOrigin, out count);
                        Logger.LogInfo($"Read GSI {messageProp.AppId}-{messageProp.MessageOrigin} result count {count}");
                        break;
                        
                    case 3:
                        var results3 = _dynamoDbReadActor.ReadLsi<TransactionLog>(messageProp.TransOrigin, messageProp.MessageType, out count);
                        Logger.LogInfo($"Read LSI {messageProp.TransOrigin}-{messageProp.MessageType} result count {count}");
                        break;
                        
                    case 4:
                        var results4 = _dynamoDbReadActor.ReadLsi2<TransactionLog>(messageProp.TransOrigin, messageProp.MessageDestination, out count);
                        Logger.LogInfo($"Read LSI2 {messageProp.TransOrigin}-{messageProp.MessageDestination} result count {count}");
                        break;
                }
                
            }).ConfigureAwait(false);
        }

        private void InitializeMessageProps()
        {
            _messageProp1 = new RoundRobinQueue<MessageProp1>(new[]
                {
                    new MessageProp1()
                    {
                        TransOrigin = "A4",
                        MessageOrigin = "A4",
                        MessageType = "UA",
                        MessageDestination = "XX",
                        AppId = "37",
                        PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix1"],
                        SortKeyRangeEnd = ConfigurationManager.AppSettings["SortKeyRangeEnd"]
                    },
                    new MessageProp1()
                    {
                        TransOrigin = "A5",
                        MessageOrigin = "A6",
                        MessageType = "HC",
                        MessageDestination = "A4",
                        AppId = "37",
                        PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix2"],
                        SortKeyRangeEnd = ConfigurationManager.AppSettings["SortKeyRangeEnd"]
                    },
                    new MessageProp1()
                    {
                        TransOrigin = "A6",
                        MessageOrigin = "A5",
                        MessageType = "UG",
                        MessageDestination = "XX",
                        AppId = "02",
                        PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix3"],
                        SortKeyRangeEnd = ConfigurationManager.AppSettings["SortKeyRangeEnd"]
                    },
                    new MessageProp1()
                    {
                        TransOrigin = "A3",
                        MessageOrigin = "XX",
                        MessageType = "CG",
                        MessageDestination = "A5",
                        AppId = "02",
                        PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix4"],
                        SortKeyRangeEnd = ConfigurationManager.AppSettings["SortKeyRangeEnd"]
                    },
                }
            );
        }
    }
}
