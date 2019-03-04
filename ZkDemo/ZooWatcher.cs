using org.apache.zookeeper;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ZkDemo
{
    internal class ZooWatcher : Watcher
    {
        private readonly Func<Task> tryReconnectDelegate;
        private readonly Func<string, Event.EventType, Task> onUpdateDelegate;
        private TaskCompletionSource<bool> connectionWaiter = new TaskCompletionSource<bool>();

        public ZooWatcher(Func<Task> tryReconnectDelegate, Func<string, Event.EventType, Task> onUpdateDelegate)
        {
            this.tryReconnectDelegate = tryReconnectDelegate;
            this.onUpdateDelegate = onUpdateDelegate;
        }

        public override async Task process(WatchedEvent @event)
        {
            if (@event.get_Type() != Event.EventType.None)
            {
                await this.onUpdateDelegate(@event.getPath(), @event.get_Type());
                return;
            }

            var state = @event.getState();
            if (state == Event.KeeperState.SyncConnected || state == Event.KeeperState.ConnectedReadOnly)
            {
                Console.WriteLine("Connected to ZooKeeper.");
                this.connectionWaiter.SetResult(true);
            }
            else if (state == Event.KeeperState.Disconnected)
            {
                Console.WriteLine("Disconnected from ZooKeeper. Autoreconnecting.");
                this.ResetWaiter();
            }
            else if (state == Event.KeeperState.Expired)
            {
                Console.WriteLine("Session is closed by ZooKeeper.");
                this.ResetWaiter();
                await this.tryReconnectDelegate();
            }
            else
            {
                throw new ZkException($"ZooKeeper connection ended up in the {state} state.");
            }
        }

        public Task WaitConnectionAsync()
        {
            return this.connectionWaiter.Task;
        }
        public void ResetWaiter()
        {
            while (true)
            {
                var tcs = this.connectionWaiter;
                if (!tcs.Task.IsCompleted ||
                    Interlocked.CompareExchange(ref this.connectionWaiter, new TaskCompletionSource<bool>(), tcs) == tcs)
                {
                    return;
                }
            }
        }
    }
}
