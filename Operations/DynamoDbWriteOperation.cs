using System;
using System.Configuration;
using System.Globalization;
using System.Threading.Tasks;
using Aamva.Ncs.LoadTestFramework.Core;
using Aamva.Ncs.LoadTestFramework.Core.Interfaces;
using Aamva.Ncs.LoadTestFramework.Core.Operations;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Helpers;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Interfaces;
using Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models;
using AAMVA.Core.Logging;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Operations
{
    public class DynamoDbWriteOperation : ParallelOperationBase
    {
        private readonly RoundRobinQueue<MessageProp1> _messageProps;

        private IDynamoDbWriteActor _dynamoDbWriteActor;
        private readonly Random _randomOps;

        public DynamoDbWriteOperation()
        {
            _messageProps = new RoundRobinQueue<MessageProp1>(new[]
            {
                new MessageProp1()
                {
                    TransOrigin = "A3",
                    MessageOrigin = "A3",
                    MessageType = "UA",
                    MessageDestination = "XX",
                    AppId = "37",
                    PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix1"]
                },
                new MessageProp1()
                {
                    TransOrigin = "A4",
                    MessageOrigin = "A6",
                    MessageType = "HC",
                    MessageDestination = "A4",
                    AppId = "37",
                    PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix2"]
                },
                new MessageProp1()
                {
                    TransOrigin = "A5",
                    MessageOrigin = "A5",
                    MessageType = "UG",
                    MessageDestination = "XX",
                    AppId = "02",
                    PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix3"]
                },
                new MessageProp1()
                {
                    TransOrigin = "A6",
                    MessageOrigin = "XX",
                    MessageType = "CG",
                    MessageDestination = "A5",
                    AppId = "02",
                    PrimarySortKey = ConfigurationManager.AppSettings["LocatorPrefix4"]
                },
            });

            _randomOps = new Random();
        }
        
        protected override void InitializeOperation(ILoadTestActor actor)
        {
            _dynamoDbWriteActor = (IDynamoDbWriteActor)actor;
        }

        protected override async Task ActAsync()
        {
            int operationNumber;
            var useAsyncWrite = ConfigurationManager.AppSettings["UseAsyncWrite"];
            if(string.IsNullOrEmpty(useAsyncWrite) || useAsyncWrite.Equals("false"))
                operationNumber = 1;
            else
            {
               operationNumber = _randomOps.Next(1, 2);
            }

            var messageProp = _messageProps.GetNextItem();
            switch (operationNumber)
            {
                case 1:
                    await Task.Run(() =>
                    {
                        _dynamoDbWriteActor.Write<TransLog>(FillTransLog(messageProp));
                        Logger.LogDebug("write done");
                    }).ConfigureAwait(false);
                    break;
                case 2:
                    await _dynamoDbWriteActor.WriteAsync<TransLog>(FillTransLog(messageProp));
                    Logger.Log("WriteAsync done");
                    break;
            }
        }

        private TransLog FillTransLog(IMessageProp messageProp)
        {
            var transLog = new TransLog()
            {
                Id = Guid.NewGuid().ToString("N"),
                AppID = 37.ToString(),
                IsInbound = true,
                LogDate = DateTime.Now.Date.ToString(CultureInfo.InvariantCulture),
                MsgDestination = messageProp.MessageDestination,
                MsgOrigin = messageProp.MessageOrigin,
                TransOrigin = messageProp.TransOrigin,
                MsgLocator = Helper.GetLocator(),
                MsgType = messageProp.MessageType,
                RequestMessage = Helper.ReturnContent(isRequest: true),
                ResponseMessage = Helper.ReturnContent(isRequest: false),
                LogTime = DateTime.Now.ToLocalTime(),
                SentTime = DateTime.Now.ToLocalTime()
            };
            return transLog;
        }
    }
}
