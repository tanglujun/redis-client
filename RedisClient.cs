using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace TFP.Redis
{
    /// <summary>Redis客户端</summary>
    /// <remarks>
    /// 以极简原则进行设计，每个客户端不支持并行命令处理，可通过多客户端多线程解决。
    /// 收发共用64k缓冲区，所以命令请求和响应不能超过64k。
    /// </remarks>
    public class RedisClient : DisposeBase
    {
        #region 属性

        /// <summary>客户端</summary>
        public TcpClient Client { get; set; }

        /// <summary>内容类型</summary>
        public IPEndPoint Server { get; set; }

        /// <summary>密码</summary>
        public String Password { get; set; }

        /// <summary>是否已登录</summary>
        public Boolean Logined { get; private set; }

        /// <summary>登录时间</summary>
        public DateTime LoginTime { get; private set; }

        /// <summary>是否正在处理命令</summary>
        public Boolean Busy { get; private set; }
        #endregion

        #region 构造
        /// <summary>销毁</summary>
        /// <param name="disposing"></param>
        protected override void OnDispose(Boolean disposing)
        {
            base.OnDispose(disposing);

            // 销毁时退出
            if (Logined)
            {
                try
                {
                    var tc = Client;
                    if (tc != null && tc.Connected && tc.GetStream() != null) Quit();
                }
                catch (ObjectDisposedException) { }
                catch (Exception ex) { }
            }

            Client.TryDispose();
        }
        #endregion

        #region 核心方法
        /// <summary>异步请求</summary>
        /// <param name="create">新建连接</param>
        /// <returns></returns>
        private Stream GetStream(Boolean create)
        {
            var tc = Client;
            NetworkStream ns = null;

            // 判断连接是否可用
            var active = false;
            try
            {
                
                ns = tc?.GetStream();//等效于if(ns!=null) tc.GetStream()
                active = ns != null && tc.Connected && ns.CanWrite && ns.CanRead;
            }
            catch (ObjectDisposedException) { }
            catch (Exception ex) {  }

            // 如果连接不可用，则重新建立连接
            if (!active)
            {
                Logined = false;
                Client = null;
                tc.TryDispose();
                if (!create) return null;

                tc = new TcpClient
                {
                    SendTimeout = 5000,
                    ReceiveTimeout = 5000
                };
                tc.Connect(Server.Address, Server.Port);

                Client = tc;
                ns = tc.GetStream();
            }

            return ns;
        }

        ///// <summary>收发缓冲区。不支持收发超过64k的大包</summary>
        //private Byte[] _Buffer;

        private static Byte[] NewLine = new[] { (Byte)'\r', (Byte)'\n' };

        /// <summary>异步发出请求，并接收响应</summary>
        /// <param name="cmd"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        protected virtual Object SendCommand(String cmd, params Packet[] args)
        {
            var isQuit = cmd == "QUIT";

            var ns = GetStream(!isQuit);
            if (ns == null) return null;

            //// 收发共用的缓冲区
            //var buf = _Buffer;
            //if (buf == null) _Buffer = buf = new Byte[64 * 1024];
            var buf = new Byte[64 * 1024];

            // 干掉历史残留数据
            var count = 0;
            if (ns is NetworkStream nss && nss.DataAvailable) count = ns.Read(buf, 0, buf.Length);

            // 验证登录
            if (!Logined && !string.IsNullOrEmpty(Password) && cmd != "AUTH")
            {
                var ars = SendCommand("AUTH", System.Text.Encoding.UTF8.GetBytes(Password));
                if (ars as String != "OK") throw new Exception("登录失败！" + ars);

                Logined = true;
                LoginTime = DateTime.Now;
            }

            // 区分有参数和无参数
            if (args == null || args.Length == 0)
            {
                //var str = "*1\r\n${0}\r\n{1}\r\n".F(cmd.Length, cmd);
                ns.Write(GetHeaderBytes(cmd, 0));
            }
            else
            {
                var ms = new MemoryStream(buf);
                ms.SetLength(0);
                ms.Position = 0;

                //var str = "*{2}\r\n${0}\r\n{1}\r\n".F(cmd.Length, cmd, 1 + args.Length);
                ms.Write(GetHeaderBytes(cmd, args.Length));

                foreach (var item in args)
                {
                    var size = item.Total;
                    var sizes =System.Text.Encoding.UTF8.GetBytes(size.ToString());
                    var len = 1 + sizes.Length + NewLine.Length * 2 + size;
                    // 防止写入内容过长导致的缓冲区长度不足的问题
                    if (ms.Position + len >= ms.Capacity)
                    {
                        // 两倍扩容
                        var ms2 = new MemoryStream(ms.Capacity * 2);
                        ms.WriteTo(ms2);
                        ms = ms2;
                    }


                    //str = "${0}\r\n".F(item.Length);
                    //ms.Write(str.GetBytes());
                    ms.WriteByte((Byte)'$');
                    ms.Write(sizes);
                    ms.Write(NewLine);
                    //ms.Write(item);
                    item.WriteTo(ms);
                    ms.Write(NewLine);

                    if (ms.Length > 1400)
                    {
                        ms.WriteTo(ns);
                        // 重置memoryStream的长度
                        ms = new MemoryStream(buf);
                        // 从头开始
                        ms.SetLength(0);
                        ms.Position = 0;
                    }
                }
                if (ms.Length > 0) ms.WriteTo(ns);
            }

            // 接收
            count = ns.Read(buf, 0, buf.Length);
            if (count == 0) return null;

            if (isQuit) Logined = false;

            /*
             * 响应格式
             * 1：简单字符串，非二进制安全字符串，一般是状态回复。  +开头，例：+OK\r\n 
             * 2: 错误信息。-开头， 例：-ERR unknown command 'mush'\r\n
             * 3: 整型数字。:开头， 例：:1\r\n
             * 4：大块回复值，最大512M。  $开头+数据长度。 例：$4\r\mush\r\n
             * 5：多条回复。*开头， 例：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
             */

            // 解析响应
            var rs = new Packet(buf, 0, count);

            var header = (Char)rs[0];

            if (header == '$') return ReadBlock(rs, ns);
            if (header == '*') return ReadBlocks(rs, ns);

            var pk = rs.Slice(1);

            var str2 = pk.ToStr().Trim();

            if (header == '+') return str2;
            if (header == '-') throw new Exception(str2);
            if (header == ':') return str2;

            throw new InvalidDataException($"无法解析响应 [{header}] [{ rs.Count}]={ rs.ToHex(32, "-")}");
        }

        private Packet ReadBlock(Packet pk, Stream ms)
        {
            var rs = ReadPacket(pk, ms);
            return rs;
        }

        private Packet[] ReadBlocks(Packet pk, Stream ms)
        {
            var header = (Char)pk[0];

            // 结果集数量
            var p = pk.IndexOf(NewLine);
            if (p <= 0) throw new InvalidDataException($"无法解析响应 {header} [{pk.Count}]");

            var n = Int32.Parse(pk.Slice(1, p - 1).ToStr());

            pk = pk.Slice(p + 2);

            var arr = new Packet[n];
            for (var i = 0; i < n; i++)
            {
                var rs = ReadPacket(pk, ms);
                arr[i] = rs;

                // 下一块，在前一块末尾加 \r\n
                pk = pk.Slice(rs.Offset + rs.Count + 2 - pk.Offset);
            }

            return arr;
        }

        private Packet ReadPacket(Packet pk, Stream ms)
        {
            var header = (Char)pk[0];

            var p = pk.IndexOf(NewLine);
            if (p <= 0) throw new InvalidDataException($"无法解析响应 [{(Byte)header}] [{pk.Count}]={pk.ToHex(32, "-")}");

            // 解析长度
            var len = Int32.Parse(pk.Slice(1, p - 1).ToStr());

            // 出错或没有内容
            if (len <= 0) return pk.Slice(p, 0);

            // 数据不足时，继续从网络流读取
            var dlen = pk.Total - (p + 2);
            var cur = pk;
            while (dlen < len)
            {
                // 需要读取更多数据，加2字节的结尾换行
                var over = len - dlen + 2;
                var count = 0;
                // 优先使用缓冲区
                if (cur.Offset + cur.Count + over <= cur.Data.Length)
                {
                    count = ms.Read(cur.Data, cur.Offset + cur.Count, over);
                    if (count > 0) cur.Set(cur.Data, cur.Offset, cur.Count + count);
                }
                else
                {
                    var buf = new Byte[over];
                    count = ms.Read(buf, 0, over);
                    if (count > 0)
                    {
                        cur.Next = new Packet(buf, 0, count);
                        cur = cur.Next;
                    }
                }
                //remain = pk.Total - (p + 2);
                dlen += count;
            }

            // 解析内容，跳过长度后的\r\n
            pk = pk.Slice(p + 2, len);

            return pk;
        }
        #endregion

        #region 主要方法
        /// <summary>执行命令。返回字符串、Packet、Packet[]</summary>
        /// <param name="cmd"></param>
        /// <param name="args"></param>
        /// <returns></returns>
       //public virtual Object Execute(String cmd, params Object[] args) => SendCommand(cmd, args.Select(e => ToBytes(e)).ToArray());

        /// <summary>执行命令。返回基本类型、对象、对象数组</summary>
        /// <param name="cmd"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public virtual string Execute(String cmd, params string[] args)
        {
            var rs = SendCommand(cmd, args.Select(e => ToBytes(e)).ToArray());
            if (rs is String str)
            {
                return rs.ToString();
            }
            if (rs is Packet pk) return pk.ToStr().Trim('\"');
            return null;
        }

        public virtual List<string> Executes(String cmd, params string[] args)
        {
            var rs = SendCommand(cmd, args.Select(e => ToBytes(e)).ToArray());
            if (rs is Packet[] pks)
            {
                List<string> list = new List<string>();
                for (var i = 0; i < pks.Length; i++)
                {
                    list.Add(pks[i].ToStr().Trim('\"'));
                }
                return list;
            }
            return null;
        }

        /// <summary>心跳</summary>
        /// <returns></returns>
        public Boolean Ping() => Execute("PING") == "PONG";

        /// <summary>选择Db</summary>
        /// <param name="db"></param>
        /// <returns></returns>
        public Boolean Select(Int32 db) => Execute("SELECT", db + "") == "OK";

        /// <summary>验证密码</summary>
        /// <param name="password"></param>
        /// <returns></returns>
        public Boolean Auth(String password) => Execute("AUTH", password) == "OK";

        /// <summary>退出</summary>
        /// <returns></returns>
        public Boolean Quit() => Execute("QUIT") == "OK";

        /// <summary>获取信息</summary>
        /// <returns></returns>
        public string GetInfo() => Execute("INFO");

        #endregion

        #region 获取设置
        /// <summary>设置</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="secTimeout">超时时间 单位秒</param>
        /// <returns></returns>
        public Boolean Set(String key, string value, Int32 secTimeout = 0)
        {
            if (secTimeout <= 0)
                return Execute("SET", key, value) == "OK";
            else
                return Execute("SETEX", key, secTimeout.ToString(), value) == "OK";
        }

        /// <summary>读取</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public string Get(String key) => Execute("GET", key);

        /// <summary>批量设置</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="values"></param>
        /// <returns></returns>
        public Boolean SetAll(IDictionary<String, String> values)
        {
            var ps = new List<Packet>();
            foreach (var item in values)
            {
                ps.Add(System.Text.Encoding.UTF8.GetBytes(item.Key));
                ps.Add(System.Text.Encoding.UTF8.GetBytes(item.Value));
            }

            var rs = SendCommand("MSET", ps.ToArray());

            return rs as String == "OK";
        }

        /// <summary>批量获取</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keys"></param>
        /// <returns></returns>
        public IDictionary<String, String> GetAll(IEnumerable<String> keys)
        {
            var ks = keys.ToArray();
            var rs = Executes("MGET", ks);

            var dic = new Dictionary<String, String>();
            for (var i = 0; i < rs.Count; i++)
            {
                dic[ks[i]] = rs[i];
            }

            return dic;
        }



        #endregion

        #region 辅助

        private static ConcurrentDictionary<String, Byte[]> _cache0 = new ConcurrentDictionary<String, Byte[]>();
        private static ConcurrentDictionary<String, Byte[]> _cache1 = new ConcurrentDictionary<String, Byte[]>();
        private static ConcurrentDictionary<String, Byte[]> _cache2 = new ConcurrentDictionary<String, Byte[]>();
        private static ConcurrentDictionary<String, Byte[]> _cache3 = new ConcurrentDictionary<String, Byte[]>();
        /// <summary>获取命令对应的字节数组，全局缓存</summary>
        /// <param name="cmd"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private static Byte[] GetHeaderBytes(String cmd, Int32 args = 0)
        {
            if (args == 0) return _cache0.GetOrAdd(cmd, k =>System.Text.Encoding.UTF8.GetBytes($"*1\r\n${k.Length}\r\n{k}\r\n"));
            if (args == 1) return _cache1.GetOrAdd(cmd, k =>System.Text.Encoding.UTF8.GetBytes($"*2\r\n${k.Length}\r\n{k}\r\n"));
            if (args == 2) return _cache2.GetOrAdd(cmd, k =>System.Text.Encoding.UTF8.GetBytes($"*3\r\n${k.Length}\r\n{k}\r\n"));
            if (args == 3) return _cache3.GetOrAdd(cmd, k =>System.Text.Encoding.UTF8.GetBytes($"*4\r\n${k.Length}\r\n{k}\r\n"));

            //return "*{2}\r\n${0}\r\n{1}\r\n".F(cmd.Length, cmd, 1 + args).GetBytes();
            return System.Text.Encoding.UTF8.GetBytes($"*{1 + args}\r\n${cmd.Length}\r\n{cmd}\r\n");
        }
        #endregion

        protected virtual Packet ToBytes(string value)
        {
            return System.Text.Encoding.UTF8.GetBytes(value);
        }


    }

}
