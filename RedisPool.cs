using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace TFP.Redis
{
    public class RedisPool : ObjectPool<RedisClient>
    {
        public string Server { get; set; }= "127.0.0.1";
        public string Password { get; set; }
        public int Port { get; set; } = 6379;
        public int DefaultDb { get; set; } = 0;


        protected override RedisClient OnCreate()
        {
            var redisClient = new RedisClient
            {
                Server = new System.Net.IPEndPoint(IPAddress.Parse(Server), Port),
                Password = Password,
            };
            if (DefaultDb > 0) redisClient.Select(DefaultDb);

            return redisClient;
        }
    }
}
