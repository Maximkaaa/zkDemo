using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ZkDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var connector = new ZkConnector();
            connector.ConnectAsync().Wait();

            StartLeaderElectionAsync(connector).Wait();

            RunQueue(connector);

            CreateCachedItem(connector);

            var exitEvent = new ManualResetEvent(false);
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                exitEvent.Set();
            };

            exitEvent.WaitOne();
        }

        public static async Task StartLeaderElectionAsync(ZkConnector connector)
        {
            await connector.JoinEllectionAsync();
        }

        public static void RunQueue(ZkConnector connector)
        {
            _ = Task.Run(async () =>
            {
                var index = 0;
                while (true)
                {
                    if (connector.IsLeader)
                    {
                        await Task.Delay(1000);

                        var itemId = $"Work item {index++}";
                        var bytes = Encoding.UTF8.GetBytes(itemId);
                        await connector.EnqueueItemAsync(bytes);
                        Console.WriteLine($"Added to queue: {itemId}");
                    }
                    else
                    {
                        var bytes = await connector.DequeueItemAsync();

                        if (bytes == null)
                        {
                            await Task.Delay(5000);
                            continue;
                        }

                        var itemId = Encoding.UTF8.GetString(bytes);
                        Console.WriteLine($"Took from the queue: {itemId}");
                    }
                }
            });
        }

        public async static void CreateCachedItem(ZkConnector connector)
        {
            var nodeName = "/watched";
            if (!await connector.NodeExistsAsync(nodeName))
            {
                await connector.CreateNodeAsync(nodeName, Guid.NewGuid().ToByteArray());
            }

            _ = Task.Run(async () =>
            {
                Guid cachedValue = default(Guid);

                while (true)
                {
                    await Task.Delay(10000);

                    if (connector.IsLeader)
                    {
                        cachedValue = Guid.NewGuid();

                        await connector.SetNodeValueAsync(nodeName, cachedValue.ToByteArray());
                    }
                    else
                    {
                        if (cachedValue == default(Guid))
                        {
                            cachedValue = new Guid(await connector.GetNodeValueAsync(nodeName, (updatedValue) =>
                            {
                                cachedValue = new Guid(updatedValue);
                                Console.WriteLine($"Updated cached value by watcher: {cachedValue}");
                            }));
                        }
                    }

                    Console.WriteLine($"Current chached value is: {cachedValue}");
                }
            });
        }
    }
}
