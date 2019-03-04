using org.apache.zookeeper;
using org.apache.zookeeper.recipes.leader;
using org.apache.zookeeper.recipes.queue;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static org.apache.zookeeper.Watcher;

namespace ZkDemo
{
    public class ZkConnector : LeaderElectionAware
    {
        private const string ZkAddress = "172.27.160.248:2181";

        private const string LeaderNode = "/leader";
        private const string QueueNode = "/queue";

        private const int MaxReconnectionAttempts = 3;

        public bool IsLeader { get; private set; }

        private readonly SemaphoreSlim connectionLock = new SemaphoreSlim(1);
        private readonly ZooWatcher watcher;
        private readonly string id;

        private ZooKeeper zk;

        private int reconnectionAttempts;
        private LeaderElectionSupport electionSupport;
        private DistributedQueue queue;

        private readonly Dictionary<string, List<Action<byte[]>>> updateHandlers = new Dictionary<string, List<Action<byte[]>>>();


        public ZkConnector()
        {
            this.watcher = new ZooWatcher(this.TryReconnectAsync, this.OnNodeUpdateAsync);
            this.id = Guid.NewGuid().ToString();
            Console.WriteLine($"My id is {this.id}.");
        }

        public async Task ConnectAsync()
        {
            await this.connectionLock.WaitAsync();

            if (this.zk != null)
            {
                await this.zk.closeAsync();
            }

            this.IsLeader = false;

            this.watcher.ResetWaiter();
            this.zk = new ZooKeeper(ZkAddress, 3000, this.watcher);
            await this.watcher.WaitConnectionAsync();

            if (this.electionSupport != null)
            {
                await this.JoinEllectionInternalAsync();
            }

            if (!await this.NodeExistsInternalAsync(QueueNode))
            {
                await this.CreateNodeInternalAsync(QueueNode);
            }

            this.queue = new DistributedQueue(this.zk, QueueNode);
            this.reconnectionAttempts = 0;

            this.connectionLock.Release();
        }

        public Task<bool> NodeExistsAsync(string path)
        {
            return this.TryAsync(() => this.NodeExistsInternalAsync(path));
        }

        private async Task<bool> NodeExistsInternalAsync(string path)
        {
            return await this.zk.existsAsync(path) != null;
        }

        public Task CreateNodeAsync(string path, byte[] data = null)
        {
            return this.TryAsync(() => this.CreateNodeInternalAsync(path, data));
        }

        public Task<byte[]> GetNodeValueAsync(string path, Action<byte[]> onUpdateHandler)
        {
            if (onUpdateHandler != null)
            {
                lock (this.updateHandlers)
                {
                    if (!this.updateHandlers.ContainsKey(path))
                    {
                        this.updateHandlers.Add(path, new List<Action<byte[]>>());
                    }

                    this.updateHandlers[path].Add(onUpdateHandler);
                }
            }

            return this.GetNodeValueAsync(path, onUpdateHandler != null);
        }

        private Task<byte[]> GetNodeValueAsync(string path, bool setNodeWatcher)
        {
            return this.TryAsync(async () =>
            {
                var nodeInfo = await this.zk.getDataAsync(path, setNodeWatcher);
                return nodeInfo.Data;
            });
        }

        public void RemoveUpdateHandler(string path, Action<byte[]> handler)
        {
            throw new NotImplementedException();
        }

        private async Task OnNodeUpdateAsync(string nodePath, Event.EventType eventType)
        {
            if (eventType != Event.EventType.NodeDataChanged)
            {
                // For the sake of this example we deal only with changing values of the nodes.
                // Other detected changes are adding/removing child nodes, creating or deleting a node.
                return;
            }

            IEnumerable<Action<byte[]>> handlers;
            lock (this.updateHandlers)
            {
                if (!this.updateHandlers.ContainsKey(nodePath))
                {
                    return;
                }

                handlers = this.updateHandlers[nodePath];
            }

            var updateValue = await this.GetNodeValueAsync(nodePath, true);

            foreach (var handler in handlers)
            {
                handler(updateValue);
            }
        }

        public Task SetNodeValueAsync(string path, byte[] data = null)
        {
            return this.TryAsync(() => this.zk.setDataAsync(path, data));
        }

        private Task<string> CreateNodeInternalAsync(string path, byte[] data = null)
        {
            return this.zk.createAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        public Task onElectionEvent(ElectionEventType eventType)
        {
            if (eventType == ElectionEventType.ELECTED_COMPLETE)
            {
                this.IsLeader = true;
            }

            Console.WriteLine($"Election process status: {eventType}");
            return Task.CompletedTask;
        }

        public Task JoinEllectionAsync()
        {
            return this.TryAsync(this.JoinEllectionInternalAsync);
        }

        private async Task<bool> JoinEllectionInternalAsync()
        {
            if (!await this.NodeExistsInternalAsync(LeaderNode))
            {
                await this.CreateNodeInternalAsync(LeaderNode);
            }

            var el = new LeaderElectionSupport(this.zk, LeaderNode, this.id);
            el.addListener(this);

            this.electionSupport = el;
            await el.start();

            return true;
        }

        public Task<string> GetLeaderNameAsync()
        {
            return this.TryAsync(this.electionSupport.getLeaderHostName);
        }

        public Task EnqueueItemAsync(byte[] data)
        {
            return this.TryAsync(() => this.queue.offer(data));
        }

        public Task<byte[]> DequeueItemAsync()
        {
            return this.TryAsync(() => this.queue.poll());
        }

        private async Task<T> TryAsync<T>(Func<Task<T>> operation)
        {
            while (true)
            {
                await this.watcher.WaitConnectionAsync();

                await this.connectionLock.WaitAsync();
                this.connectionLock.Release();

                try
                {
                    return await operation();
                }
                catch (KeeperException.ConnectionLossException)
                {
                    // Skip the error and try to reconnect. If reconnection failes after set number of attempts, fail with another error.
                }
                catch (KeeperException.SessionExpiredException)
                {
                    // Skip the error and try to reconnect. If reconnection failes after set number of attempts, fail with another error.
                }
            }
        }

        private Task TryReconnectAsync()
        {
            if (++this.reconnectionAttempts > MaxReconnectionAttempts)
            {
                throw new ZkException($"Failed to reconnect to ZooKeeper after {MaxReconnectionAttempts} attempts.");
            }

            Console.WriteLine($"Attempting to reconnect to ZooKeeper. Attempt number {this.reconnectionAttempts}...");

            return this.ConnectAsync();
        }
    }
}
