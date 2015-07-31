using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Producer
{
    public class Program
    {
        public void Main(string[] args)
        {
            var options = GetOptions(args);
            if (options == null) return;
            
            var count = 0;
            var lastCount = 0;
            var reporter = new Task(() =>
            {
                while (true)
                {
                    var current = count;
                    Console.WriteLine("{0} messages in last second.", current - lastCount);
                    lastCount = current;
                    Thread.Sleep(1000);
                }
            });
            var kafkaOptions = new KafkaOptions(options.KafkaNodeUri);// { Log = new ConsoleLog() };
            using (var router = new BrokerRouter(kafkaOptions))
            using (var client = new KafkaNet.Producer(router))
            {
                reporter.Start();
                while (true)
                {
                    Thread.Sleep(100);
                    client.SendMessageAsync("TestHarness", new[] { new Message() { Value = BitConverter.GetBytes(DateTime.Now.Ticks) } });
                    count++;
                }
            }
        }
        
        static Options GetOptions(string[] args) {
            var options = new Options();
            if (args.Length == 0 || (args.Length == 1 && args[0] == "help")) {
                WriteHelp();
                return null;
            }
            
            if (args.Length >= 1) {
                try {
                    options.KafkaNodeUri = new Uri(args[0]);                    
                } catch (Exception ex) {
                    Console.Error.WriteLine(ex.ToString());
                    Console.Error.WriteLine("Hmm... you need to give me a valid URI for a Kafka node... Maybe try asking me for help.");
                    return null;
                }
            }
                
            var sleep = 0;
            if (args.Length >= 2) {
                if (!int.TryParse(args[1], out sleep))
                {
                    Console.Error.WriteLine("Hmm... you need to give me a valid number of milliseconds to sleep for... Maybe try asking me for help.");
                    return null;
                }
                options.SleepDuration = sleep;
            }
            
            return options;
        }
        
        static void WriteHelp()
        {
            Console.WriteLine("Hello. I'm the Kafka Producer. I... produce messages.");
            Console.WriteLine("Just tell me where kafka is and how long to sleep between each message.");
            Console.WriteLine("\tE.g. \"dnx . Producer http://192.168.59.103:9092 100\" will publish one message to a kafka install at 192.168.59.103 on port 9092 every 100ms");
            Console.WriteLine("If you don't tell me where kafka is we're going to have a bit of a problem...");
            Console.WriteLine("If you don't tell me how long to wait I just won't wait at all! :-)");
        }
    }
    
    class Options {
        public Uri KafkaNodeUri { get; set; }
        public int SleepDuration { get; set; }
    }
}
