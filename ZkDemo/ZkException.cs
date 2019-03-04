using System;

namespace ZkDemo
{
    internal class ZkException : Exception
    {
        public ZkException(string message) : base(message)
        {
        }
    }
}
