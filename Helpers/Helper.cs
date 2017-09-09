using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aamva.Ncs.LoadTestFramework.DynamoDbActors.Helpers
{
    public class Helper
    {
        public static byte[] ReturnContent(bool isRequest)
        {
            var useXmlPayload = ConfigurationManager.AppSettings["UseXmlPayload"];
            if (String.IsNullOrEmpty(useXmlPayload) || useXmlPayload.Equals("false"))
            {
                return Encoding.ASCII.GetBytes(isRequest ? "my string" : "my response");
            }
            return ReadFile(isRequest ? "NiemMessageBody6.xml" : "NiemMessageBody5.xml");
        }

        private static string ReadFilePath()
        {
            string path = Directory.GetCurrentDirectory();
            string xmlfilePath = path + "\\xml";
            return xmlfilePath;
        }

        private static byte[] ReadFile(string filename)
        {
            string path = ReadFilePath();
            string xmlPath = path + "\\" + filename;

            byte[] contBytes = File.ReadAllBytes(xmlPath);

            return contBytes;
        }

        public static string GetLocator()
        {
            string locator = DateTime.Now.Date.ToLongDateString() + ":" + DateTime.Now.ToLongTimeString() + ":" +
                             new Random().NextDouble().ToString("N");

            return locator;
        }
    }
}
