using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models
{
 
    [DynamoDBTable("TransLog")] //use this with sort key "SortKeyId"
    public class TransLog
    {
        public TransLog()
        {

        }

        public Int64 SortKeyId { get; set; }

        public string Id { get; set; }

        public string AppID { get; set; }
        public bool IsInbound { get; set; }

        public DateTime LogTime { get; set; }
        public string LogDate { get; set; }

        public string MsgDestination { get; set; }

        public string MsgLocator { get; set; }

        public string MsgOrigin { get; set; }

        public string MsgType { get; set; }

        public DateTime? ReceivedTime { get; set; }

        public byte[] RequestMessage { get; set; }

        public byte[] ResponseMessage { get; set; }

        public DateTime? SentTime { get; set; }

        [DynamoDBHashKey]
        public string TransOrigin { get; set; }

    }
}
