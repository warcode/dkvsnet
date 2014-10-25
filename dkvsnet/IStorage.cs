using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dkvsnet
{
    /// <summary>
    /// Storage instructs Raft to achieve consensus on the storage of a specified key/value pair.
    /// 
    /// </summary>
    interface IStorage
    {
        bool Set(string key, string value);
        string Get(string key);

        bool Persist(string key, string value);

    }
}
