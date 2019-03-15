using System;
using System.Collections.Generic;
using System.Text;
using Adf;

namespace Adf.NumberServer
{
    class NumberManager
    {
        private const int DEFAULT_SIZE = 10000;
        private readonly int capacity = DEFAULT_SIZE;
        //
        private int[] buckets = new int[DEFAULT_SIZE];
        //slots
        private int[] hashCodes = new int[DEFAULT_SIZE];
        private int[] nexts = new int[DEFAULT_SIZE];
        private string[] keys = new string[DEFAULT_SIZE];
        private DataItem[] values = new DataItem[DEFAULT_SIZE];
        //
        private int size = DEFAULT_SIZE;
        private int count = 0;
        private int lastIndex = 0;
        private int freeIndex = -1;
        //
        public readonly object SyncObject = new object();

        //
        public int Count
        {
            get { return this.count; }
        }

        //
        public NumberManager()
        {
            this.capacity = Adf.ConfigHelper.GetSettingAsInt("Capacity", DEFAULT_SIZE);
            if (this.capacity < 100)
            {
                throw new Adf.ConfigException("Capacity configuration invalid, limit 100+ ");
            }
        }

        public DataItem Get(string key, int hashCode)
        {
            int bucket = hashCode % this.size;
            //
            for (int i = this.buckets[bucket] - 1; i >= 0; i = this.nexts[i])
            {
                if (this.hashCodes[i] == hashCode && this.keys[i] == key)
                {
                    return this.values[i];
                }
            }
            //
            return null;
        }

        public bool Remove(string key, int hashCode)
        {
            int bucket = hashCode % this.size;
            int last = -1;
            for (int i = this.buckets[bucket] - 1; i >= 0; last = i, i = this.nexts[i])
            {
                if (this.hashCodes[i] == hashCode && this.keys[i] == key)
                {
                    if (last < 0)
                    {
                        this.buckets[bucket] = this.nexts[i] + 1;
                    }
                    else
                    {
                        this.nexts[last] = this.nexts[i];
                    }
                    this.hashCodes[i] = -1;
                    this.keys[i] = null;
                    this.values[i] = null;
                    this.nexts[i] = this.freeIndex;
                    //
                    this.count--;
                    if (this.count == 0)
                    {
                        this.lastIndex = 0;
                        this.freeIndex = -1;
                    }
                    else
                    {
                        this.freeIndex = i;
                    }
                    //
                    return true;
                }
            }
            //
            return false;
        }

        public bool Add(string key, int hashCode, DataItem item)
        {
            int bucket = hashCode % this.size;
            for (int i = this.buckets[bucket] - 1; i >= 0; i = this.nexts[i])
            {
                if (this.hashCodes[i] == hashCode && this.keys[i] == key)
                {
                    //exists
                    return false;
                }
            }
            //
            int index;
            if (this.freeIndex >= 0)
            {
                index = this.freeIndex;
                this.freeIndex = this.nexts[index];
            }
            else
            {
                if (this.lastIndex == this.size)
                {
                    ResetCapacity();
                    bucket = hashCode % this.size;
                }
                index = this.lastIndex;
                this.lastIndex++;
            }
            //
            this.hashCodes[index] = hashCode;
            this.keys[index] = key;
            //this.values[index] = new DataItem() { key = key, value = value };
            this.values[index] = item;
            this.nexts[index] = this.buckets[bucket] - 1;
            //
            this.buckets[bucket] = index + 1;
            this.count++;
            //
            return true;
        }

        public void Set(string key, int hashCode, DataItem item)
        {
            int bucket = hashCode % this.size;
            for (int i = this.buckets[bucket] - 1; i >= 0; i = this.nexts[i])
            {
                if (this.hashCodes[i] == hashCode && this.keys[i] == key)
                {
                    //exists
                    this.values[i] = item;
                    return;
                }
            }
            //
            int index;
            if (this.freeIndex >= 0)
            {
                index = this.freeIndex;
                this.freeIndex = this.nexts[index];
            }
            else
            {
                if (this.lastIndex == this.size)
                {
                    ResetCapacity();
                    bucket = hashCode % this.size;
                }
                index = this.lastIndex;
                this.lastIndex++;
            }
            //
            this.hashCodes[index] = hashCode;
            this.keys[index] = key;
            //this.values[index] = new DataItem() { key = key, value = value };
            this.values[index] = item;
            this.nexts[index] = this.buckets[bucket] - 1;
            //
            this.buckets[bucket] = index + 1;
            this.count++;
        }

        private void ResetCapacity()
        {
            int newSize = this.size + this.capacity;

            //            
            int[] newHashCodes = new int[newSize];
            int[] newNexts = new int[newSize];
            string[] newKeys = new string[newSize];
            DataItem[] newValues = new DataItem[newSize];

            //
            Array.Copy(this.hashCodes, 0, newHashCodes, 0, this.lastIndex);
            Array.Copy(this.nexts, 0, newNexts, 0, this.lastIndex);
            Array.Copy(this.keys, 0, newKeys, 0, this.lastIndex);
            Array.Copy(this.values, 0, newValues, 0, this.lastIndex);

            //
            int[] newBuckets = new int[newSize];
            int bucket = 0;
            for (int i = 0; i < this.lastIndex; i++)
            {
                bucket = newHashCodes[i] % newSize;
                newNexts[i] = newBuckets[bucket] - 1;
                newBuckets[bucket] = i + 1;
            }

            //
            this.buckets = newBuckets;
            //
            this.hashCodes = newHashCodes;
            this.nexts = newNexts;
            this.keys = newKeys;
            this.values = newValues;
            //
            this.size = newSize;
        }
        
        ////允许脏读
        //public DataItem[] GetLastProperty(int size)
        //{
        //    var list = new List<DataItem>(size);
        //    for (int i = this.lastIndex; i >= 0; i--)
        //    {
        //        var item = this.values[i];
        //        if (item != null)
        //        {
        //            list.Add(item);
        //            //
        //            if (list.Count >= size)
        //            {
        //                break;
        //            }
        //        }
        //    }
        //    return list.ToArray();
        //}
        
        public void GetItems(Action<DataItem> action,ref int counter, int size)
        {
            for (int i = 0, l = this.values.Length;
                i < l && counter < size;
                i++)
            {
                var v = this.values[i];
                if (v != null)
                {
                    action(v);
                }
            }
        }

        public static NumberManager GetManager(string key, int hashCode)
        {
            var slot = hashCode % Program.HASH_POOL_SIZE;
            var cacheManager = Program.NumberManagers[slot];
            return cacheManager;
        }
    }
}