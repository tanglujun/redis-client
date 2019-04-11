using System;
using System.Collections.Generic;
using System.Text;

namespace TFP.Redis
{
    public interface IRedis
    {
        /// <summary>为同一服务器创建不同Db的子级库</summary>
        /// <param name="db"></param>
        /// <returns></returns>
        Redis CreateSub(Int32 db);

        /// <summary>单个实体项</summary>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        Boolean Set(String key, String value, Int32 expire = -1);

        /// <summary>获取单体</summary>
        /// <param name="key">键</param>
        String Get(String key);

        /// <summary>批量移除缓存项</summary>
        /// <param name="keys">键集合</param>
        Int32 Remove(params String[] keys);

        /// <summary>是否存在</summary>
        /// <param name="key">键</param>
        Boolean ContainsKey(String key);

        /// <summary>设置缓存项有效期</summary>
        /// <param name="key">键</param>
        /// <param name="expire">过期时间</param>
        Boolean SetExpire(String key, TimeSpan expire);

        /// <summary>获取缓存项有效期</summary>
        /// <param name="key">键</param>
        /// <returns></returns>
        TimeSpan GetExpire(String key);


        #region 集合操作
        /// <summary>批量获取缓存项</summary>
        /// <typeparam name="String"></typeparam>
        /// <param name="keys"></param>
        /// <returns></returns>
        IDictionary<String, String> GetAll(IEnumerable<String> keys);

        /// <summary>批量设置缓存项</summary>
        /// <param name="values"></param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        void SetAll(IDictionary<String, String> values, Int32 expire = -1);

        #endregion

        /// <summary>添加，已存在时不更新</summary>
        /// <typeparam name="String">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <param name="expire">过期时间，秒。小于0时采用默认缓存时间<seealso cref="Cache.Expire"/></param>
        /// <returns></returns>
        Boolean Add(String key, String value, Int32 expire = -1);

        /// <summary>设置新值并获取旧值，原子操作</summary>
        /// <typeparam name="String">值类型</typeparam>
        /// <param name="key">键</param>
        /// <param name="value">值</param>
        /// <returns></returns>
        String Replace(String key, String value);
        /// <summary>累加，原子操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        Int64 Increment(String key, Int64 value);

        /// <summary>累加，原子操作，乘以100后按整数操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        Double Increment(String key, Double value);

        /// <summary>递减，原子操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        Int64 Decrement(String key, Int64 value);

        /// <summary>递减，原子操作，乘以100后按整数操作</summary>
        /// <param name="key">键</param>
        /// <param name="value">变化量</param>
        /// <returns></returns>
        Double Decrement(String key, Double value);

        #region 哈希操作
        String GetHash(String key, String hashKey);

        Boolean SetHash(String key, String hashKey,String value);
        #endregion

        String GetInfo();
    }
}
