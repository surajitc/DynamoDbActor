using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Server;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Models
{
    public interface IMessageProp
    {
        string TransOrigin { get; set; }
        string MessageOrigin { get; set; }
        string MessageDestination { get; set; }
        string MessageType { get; set; }

        string AppId { get; set; }

        string PrimarySortKey { get; set; }

        string SortKeyRangeEnd { get; set; }

        TOutput GetPrimarySortKeyPrefix<TOutput>();

        TOutput GetSortKeyRangeEnd<TOutput>() ;


    }

    public class MessageProp1 : IMessageProp
    {
        private object _priSortKey;
        private object _sortKeyRange;
        public string TransOrigin { get; set; }
        public string MessageOrigin { get; set; }
        public string MessageDestination { get; set; }
        public string MessageType { get; set; }
        public string AppId { get; set; }

        public string PrimarySortKey { get; set; }

        public string SortKeyRangeEnd { get; set; }

        public TOutput GetPrimarySortKeyPrefix<TOutput>() 
        {
            if (_priSortKey == null)
            {
                var converter = TypeDescriptor.GetConverter(typeof(string));
                _priSortKey = converter.ConvertTo(PrimarySortKey, typeof(TOutput));
                return (TOutput) _priSortKey;
            }
            return (TOutput)_priSortKey;
        }

        public TOutput GetSortKeyRangeEnd<TOutput>() 
        {
            if (_sortKeyRange == null)
            {
                var converter = TypeDescriptor.GetConverter(typeof(string));

                _sortKeyRange = converter.ConvertTo(SortKeyRangeEnd, typeof(TOutput));
                return (TOutput) _sortKeyRange;
            }
            return (TOutput) _sortKeyRange;
        }

    }

    public class MessageProp2 : IMessageProp
    {
        private object _priSortKey;
        private object _sortKeyRange;
        public string TransOrigin { get; set; }
        public string MessageOrigin { get; set; }
        public string MessageDestination { get; set; }
        public string MessageType { get; set; }

        public string AppId { get; set; }

        public string PrimarySortKey { get; set; }

        public string SortKeyRangeEnd { get; set; }

        public TOutput GetPrimarySortKeyPrefix<TOutput>() 
        {
            if (_priSortKey == null)
            {
                var converter = TypeDescriptor.GetConverter(typeof(Int64));
                _priSortKey = converter.ConvertTo(PrimarySortKey, typeof(TOutput));
                return (TOutput) _priSortKey;
            }
            return (TOutput) _priSortKey;
        }

        public TOutput GetSortKeyRangeEnd<TOutput>() 
        {

            if (_sortKeyRange == null)
            {
                var converter = TypeDescriptor.GetConverter(typeof(long));

                _sortKeyRange = converter.ConvertTo(SortKeyRangeEnd, typeof(TOutput));
                return (TOutput) _sortKeyRange;
            }
            return (TOutput) _sortKeyRange;

        }

    }

}
