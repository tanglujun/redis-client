using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
namespace TFP.Redis
{
    /// <summary>Redis缓存</summary>
    public class Redis : DisposeBase,IRedis
    {
        #region 静态
        /// <summary>
        /// 创建
        /// </summary>
        /// <param name="server"></param>
        /// <param name="password"></param>
        /// <param name="db"></param>
        /// <returns></returns>
        public static IRedis Create(String server, String password, Int32 db)
        {
            return new Redis { Server = server, Password = password, Db = db };
        }
        #endregion

        #region 属性
        /// <summary>服务器</summary>
        public String Server { get; set; } = "127.0.0.1";

        /// <summary>密码</summary>
        public String Password { get; set; }

        public int Port { get; set; } = 6379;

        /// <summary>目标数据库。默认0</summary>
        public Int32 Db { get; set; } = 0;

        /// <summary>出错重试次数。如果出现协议解析错误，可以重试的次数，默认0</summary>
        public Int32 Retry { get; set; }
        #endregion

        #region 构造

        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            _Pool.TryDispose();
        }

        /// <summary>已重载。</summary>
        /// <returns></returns>
        //public override String ToString() => $"{Name} Server={Server} Db={Db}";

        #endregion

        #region 子库
        private ConcurrentDictionary<Int32, Redis> _sub = new ConcurrentDictionary<Int32, Redis>();
        /// <summary>为同一服务器创建不同Db的子级库</summary>
        /// <param name="db"></param>
        /// <returns></returns>
        public virtual Redis CreateSub(Int32 db)
        {
            if (Db != 0) throw new ArgumentOutOfRangeException(nameof(Db), "只有Db=0的库才能创建子级库连接");
            if (db == 0) return this;

            return _sub.GetOrAdd(db, k =>
            {
                var r = new Redis
                {
                    Server = Server,
                    Db = db,
                    Password = Password,
                };
                return r;
            });
        }
        #endregion

        #region 客户端池

        private RedisPool _Pool;
        /// <summary>连接池</summary>
        public IPool<RedisClient> Pool
        {
            get
            {
                if (_Pool != null) return _Pool;
                lock (this)
                {
                    if (_Pool != null) return _Pool;

                    var pool = new RedisPool
                    {
                        Name = "RedisPool",
                        Server = Server,
                        Port = Port,
                        Password = Password,
                        DefaultDb = Db,
                        Min = 2,
                        Max = 1000,
                        IdleTime = 20,
                        AllIdleTime = 120
                    };

                    return _Pool = pool;
                }
            }
        }

        /// <summary>执行命令</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func"></param>
        /// <returns></returns>
        public virtual T Execute<T>(Func<RedisClient, T> func)
        {
            var client = Pool.Get();
            try
            {
                var i = 0;
                do
                {
                    try
                    {
                        var rs = func(client);
                        //// 如果返回Packet，需要在离开对象池之前拷贝，否则可能出现冲突
                        //if ((Object)rs is Packet pk) return (T)(Object)pk.Clone();

                        return rs;
                    }
                    catch (InvalidDataException)
                    {
                        if (i++ >= Retry) throw;
                    }
                } while (true);
            }
            finally
            {
                Pool.Put(client);
            }
        }

        #endregion

        #region 基础操作
        /// <summary>缓存个数</summary>
        public Int32 Count
        {
            get
            {
                var client = Pool.Get();
                try
                {
                    return Convert.ToInt32(client.Execute("DBSIZE"));
                }
                finally
                {
                    Pool.Put(client);
                }
            }
        }

        /// <summary>所有键</summary>
        public ICollection<String> Keys
        {
            get
            {
                if (Count > 10000) throw new InvalidOperationException("数量过大时，禁止获取所有键");

                var client = Pool.Get();
                try
                {
                    var rs = client.Execute("KEYS", "*");
                    return rs.Split(new string[] { Environment.NewLine },StringSplitOptions.RemoveEmptyEntries).ToList();
                }
                finally
                {
                    Pool.Put(client);
                }
            }
        }

        /// <summary>单个实体项</summary>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        public Boolean Set(String key, String value, Int32 expire = -1)
        {
            if (expire < 0) expire = 0;

            return Execute(rds => rds.Set(key, value, expire));
        }

        /// <summary>获取单体</summary>
        /// <param name="key">键</param>
        public String Get(String key) => Execute(rds => rds.Get(key));

        /// <summary>批量移除缓存项</summary>
        /// <param name="keys">键集合</param>
        public Int32 Remove(params String[] keys) => Execute(rds => Convert.ToInt32(rds.Execute("DEL", keys)));

        /// <summary>是否存在</summary>
        /// <param name="key">键</param>
        public Boolean ContainsKey(String key) => Execute(rds => Convert.ToInt32(rds.Execute("EXISTS", key)) > 0);

        /// <summary>设置缓存项有效期</summary>
        /// <param name="key">键</param>
        /// <param name="expire">过期时间</param>
        public Boolean SetExpire(String key, TimeSpan expire) => Execute(rds => rds.Execute("EXPIRE", key, expire.TotalSeconds.ToString()) == "1");

        /// <summary>获取缓存项有效期</summary>
        /// <param name="key">键</param>
        /// <returns></returns>
        public TimeSpan GetExpire(String key)
        {
            var sec = Execute(rds => Convert.ToInt32(rds.Execute("TTL", key)));
            return TimeSpan.FromSeconds(sec);
        }
        #endregion

        #region 集合操作
        /// <summary>批量获取缓存项</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keys"></param>
        /// <returns></returns>
        public IDictionary<String, String> GetAll(IEnumerable<String> keys) => Execute(rds => rds.GetAll(keys));

        /// <summary>批量设置缓存项</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values"></param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        public void SetAll(IDictionary<String, String> values, Int32 expire = -1)
        {
            if (expire > 0) throw new ArgumentException("批量设置不支持过期时间", nameof(expire));

            Execute(rds => rds.SetAll(values));
        }

        #endregion

        #region 高级操作
        /// <summary>添加，已存在时不更新</summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        /// <returns></returns>
        public Boolean Add(String key, String value, Int32 expire = -1) => Execute(rds =>Convert.ToInt32(rds.Execute("SETNX", key, value)) == 1);

        /// <summary>设置新值并获取旧值，原子操作</summary>
        /// <typeparam name="T">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        public string Replace(String key, String value) => Execute(rds => rds.Execute("GETSET", key, value));

        /// <summary>累加，原子操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        public Int64 Increment(String key, Int64 value)
        {
            if (value == 1)
                return Execute(rds => Convert.ToInt64(rds.Execute("INCR", key)));
            else
                return Execute(rds => Convert.ToInt64(rds.Execute("INCRBY", key, value.ToString())));
        }

        /// <summary>累加，原子操作，乘以100后按整数操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        public Double Increment(String key, Double value) => Execute(rds => Convert.ToDouble(rds.Execute("INCRBYFLOAT", key, value.ToString())));

        /// <summary>递减，原子操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        public Int64 Decrement(String key, Int64 value)
        {
            if (value == 1)
                return Execute(rds => Convert.ToInt64(rds.Execute("DECR", key)));
            else
                return Execute(rds => Convert.ToInt64(rds.Execute("DECRBY", key, value.ToString())));
        }

        /// <summary>递减，原子操作，乘以100后按整数操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        public Double Decrement(String key, Double value)
        {
            //return (Double)Decrement(key, (Int64)(value * 100)) / 100;
            return Increment(key, -value);
        }
        #endregion

        public String GetHash(String key, String hashKey)
        {
            return Execute(rds => rds.Execute("HGET", key, hashKey));
        }

        public Boolean SetHash(String key, String hashKey, String value)
        {
            return Execute(rds => Convert.ToInt32(rds.Execute("HSET", key, hashKey, value))==1 );
        }

        public String GetInfo()
        {
            return Execute(rds => rds.Execute("INFO"));
        }
    }
}