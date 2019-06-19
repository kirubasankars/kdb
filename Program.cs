using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;

namespace kdb3
{
    class Program
    {
        static void Main(string[] args)
        {

            
            while(true) {
                
                var kdb = new KDB(".");


                Console.WriteLine("...........");
                Console.WriteLine("1. Get Key");
                Console.WriteLine("2. Put Key");
                Console.WriteLine("3. Read All");
                Console.WriteLine("...........");

                var input = Console.ReadLine();

                if(input == "1") {
                    var inputKey = Console.ReadLine();
                    Console.WriteLine(Encoding.ASCII.GetString(kdb.Get(inputKey)));
                } 
                if(input == "2") {
                    var inputKey = Console.ReadLine();
                    var inputValue = Console.ReadLine();
                    kdb.Put(inputKey, Encoding.ASCII.GetBytes(inputValue));
                    
                } 
                if(input == "3") {
                    var iterator = kdb.GetAll();
                    while(true) {
                        var key = iterator.Next();
                        if (key == null) { break; }
                        Console.WriteLine("key : " + key);
                        Console.WriteLine("val : " + Encoding.ASCII.GetString(kdb.Get(key)));
                    }
                } 

                kdb.Close();
            }

        }
    }

    struct DataPointer
    {
        public string _key;
        public int _keyLen, _valueLen;
        public long _postion;
        public DataPointer(string key, int keyLen, int valueLen, long postion)
        {
            _key = key;
            _keyLen = keyLen;
            _valueLen = valueLen;
            _postion = postion;
        }
    }
    interface IndexTable
    {
        void Put(string key, DataPointer dataPointer);
        DataPointer Get(string key);
        int Count();
	IDictionary GetStore();
    }
    
    class MemTable : IndexTable
    {        
        private readonly KDB _kDB;        
        private bool _persisted = false;
        private int _totalKeyLength = 0;
        public int TotalKeyLength { get { return _totalKeyLength; } }
        public SortedDictionary<string, DataPointer> Store { get; private set; }

        public MemTable(KDB kDB)
        {
            _kDB = kDB ?? throw new ArgumentNullException(nameof(kDB));            
            Store = new SortedDictionary<string, DataPointer>();
        }

        public void Put(string key, DataPointer dataPointer)
        {
            _totalKeyLength += dataPointer._keyLen;
            Store[key] = dataPointer;
        }

        public DataPointer Get(string key)
        {
            DataPointer dataPointer = default(DataPointer);
            if (Store.TryGetValue(key, out dataPointer)) return dataPointer;
            return dataPointer;
        }

        public int Count()
        {
            return Store.Count;
        }

	public IDictionary GetStore() {
	    return Store;
	}

        public void Persist(int fileNumber)
        {
            if (_persisted || Store.Count == 0) return;

            _kDB.IndexStorage.Persist(this, fileNumber);

            _persisted = true;
        }
    }

    class SSTable : IndexTable
    {
        private readonly SortedList<string, DataPointer> _store;
        private readonly KDB _kDB;
        private readonly int _fileNumber;

        public SSTable(KDB kDB, int fileNumber)
        {
            _kDB = kDB;
            _store = new SortedList<string, DataPointer>();
            _fileNumber = fileNumber;

            LoadFile(fileNumber);
        }
        public DataPointer Get(string key)
        {
            DataPointer dataPointer = default(DataPointer);
            if (_store.TryGetValue(key, out dataPointer)) return dataPointer;
            return dataPointer;
        }
        public void Put(string key, DataPointer dataPointer)
        {
            throw new NotSupportedException();
        }
        public int Count()
        {
            return _store.Count;
        }

	public IDictionary GetStore() {
	    return _store;
	}

        private void LoadFile(int fileNumber)
        {
            var indexFile = new FileStream(_kDB.DBPath + "/Index-" + fileNumber.ToString() + ".db", FileMode.Open);

            byte[] data = new byte[indexFile.Length];
            indexFile.Read(data, 0, data.Length);

            var itemAt = 0;

            while (data.Length > itemAt)
            {
                var keyBytesLength = BitConverter.ToInt64(data, itemAt);
                byte[] keyBytes = new byte[keyBytesLength];
                for (int i = itemAt + 64 + 1, j = 0; i <= (64 + itemAt + keyBytesLength); i++, j++)
                {
                    keyBytes[j] = data[i];
                }
                string key = Encoding.ASCII.GetString(keyBytes);
                var position = BitConverter.ToInt64(data, itemAt + 64 + (int)keyBytesLength + 1);
                var valueLength = (int)BitConverter.ToInt64(data, itemAt + 64 + (int)keyBytesLength + 64 + 1);

                _store.Add(key, new DataPointer(key, key.Length, valueLength, position));

                itemAt += (64 + (int)keyBytesLength + 64 + 64 + 1);
            }

            indexFile.Close();
        }
    }

    class IndexTableIterator {
        private IndexTable _indexTable = null;
	    private IDictionaryEnumerator _enumerator = null;
        private bool exists = false;
        public IndexTableIterator (IndexTable indexTable, int priority) {
            this._indexTable = indexTable;
	        this._enumerator = indexTable.GetStore().GetEnumerator();
	        exists = this._enumerator.MoveNext();
            this.Priority = priority;
        }

        public string Peek() {
            if (exists && this._enumerator.Current != null) {
                return ((DictionaryEntry)this._enumerator.Current).Key.ToString();
            }
            return null;
        }

        public string Next() {
            exists = this._enumerator.MoveNext();
	        if (exists) {
                return ((DictionaryEntry)this._enumerator.Current).Key.ToString(); 
            } 
            return null;
        }

        public int Priority { get; private set; }
    }

    class MergeIndexTableIterator {
        private SortedDictionary<string, IndexTableIterator> _mergeList = new SortedDictionary<string, IndexTableIterator>();
        private List<IndexTableIterator> _indexTableIterators = new List<IndexTableIterator>();
        public MergeIndexTableIterator(LinkedList<IndexTable> linkedList) {
            var item = linkedList.First;
            int len = linkedList.Count;
            while (item != null)
            {
                IndexTableIterator indexTableIterator = null;
                if(item.Value.GetType() == typeof(SSTable)){
                    indexTableIterator = new IndexTableIterator(item.Value, len);   
                }
                if(item.Value.GetType() == typeof(MemTable)){
                    indexTableIterator = new IndexTableIterator(item.Value, len);   
                }

                if (indexTableIterator.Peek() != null) {
                        _indexTableIterators.Add(indexTableIterator);
                    }
                item = item.Next;
                len --;
            }

            foreach(var iterator in _indexTableIterators) {
                var item1 = iterator.Peek();
                if (_mergeList.ContainsKey(item1)) {
                    if(_mergeList[item1].Priority < iterator.Priority) {
                        _mergeList[item1] = iterator;
                    }
                } else {
                    _mergeList.Add(item1, iterator);
                }
            }
            
        }

        public string Next() {
            if (_mergeList.Count <= 0) return null;

            var item  = _mergeList.First();
            var key = item.Key;
            var iterator = item.Value;

            _mergeList.Remove(key);
            var nextKey = iterator.Next();
            if (nextKey  != null) {
                if (_mergeList.ContainsKey(nextKey)) {
                    if(_mergeList[nextKey].Priority < iterator.Priority) {
                        _mergeList[nextKey] = iterator;
                    }
                } else {
                    _mergeList.Add(nextKey, iterator);
                }
            }

            return key;
        }

    }
    
    class IndexManager
    {
        private MemTable _memTable = null;
        private LinkedList<IndexTable> _dlinkedList = null;
        private int _lastIndexFileNumber = 0;
        private readonly KDB _kDB;        

        public IndexManager(KDB kDB)
        {
            _kDB = kDB ?? throw new ArgumentNullException(nameof(kDB));
            _lastIndexFileNumber = GetLastFileNumber();

            _dlinkedList = new LinkedList<IndexTable>();

            LoadSSTables();

            _memTable = new MemTable(kDB);
            _dlinkedList.AddFirst(_memTable);
        }

        private void LoadSSTables()
        {
            var dataFolder = new DirectoryInfo(_kDB.DBPath);
            var indexFiles = dataFolder.GetFiles("Index-*");
            var values = indexFiles.Select(f => int.Parse(f.Name.Replace("Index-", "").Replace(".db", ""))).ToList();
            values.Sort();

            foreach (var fileNum in values)
            {
                var ssTable = new SSTable(_kDB, fileNum);
                _dlinkedList.AddFirst(ssTable);
            }
        }

        private int GetLastFileNumber()
        {
            var dataFolder = new DirectoryInfo(_kDB.DBPath);
            var indexFiles = dataFolder.GetFiles("Index-*");
            var values = indexFiles.Select(f => int.Parse(f.Name.Replace("Index-", "").Replace(".db", ""))).ToList();
            if (values.Count == 0) return 0;
            return values.Max();
        }

        public void Put(string key, DataPointer dataPointer)
        {
            if (_memTable.Count() >= 100000)
            {
                _memTable.Persist(++_lastIndexFileNumber);                
                _memTable = new MemTable(_kDB);
                _dlinkedList.AddFirst(_memTable);
            }
            _memTable.Put(key, dataPointer);
        }

        public DataPointer Get(string key)
        {
            var item = _dlinkedList.First;
            while (item != null)
            {
                var dataPointer = item.Value.Get(key);
                if (dataPointer._keyLen != 0) return dataPointer;

                item = item.Next;
            }
            return default(DataPointer);
        }

        public MergeIndexTableIterator GetAll() {
            return new MergeIndexTableIterator(_dlinkedList);
        }

        public void Close()
        {
            _memTable.Persist(++_lastIndexFileNumber);
        }
    }
    class KDB
    {
        private readonly DataStorage _dataStorage;
        private readonly IndexStorage _indexStorage;
        private readonly IndexManager _indexManager;

        public KDB(string dbPath)
        {
            DBPath = dbPath;
            _dataStorage = new DataStorage(this);
            _indexStorage = new IndexStorage(this);
            _indexManager = new IndexManager(this);
        }

        public string DBPath { get; private set; }

        public void Put(string key, byte[] value)
        {
            var dataPointer = _dataStorage.Put(key, value);
            _indexManager.Put(key, dataPointer);
        }

        public byte[] Get(string key)
        {
            var itemPointer = _indexManager.Get(key);
            return _dataStorage.Get(itemPointer);
        }
        
        public MergeIndexTableIterator GetAll() {
            return _indexManager.GetAll();
        }
        internal IndexStorage IndexStorage => _indexStorage;

        public void Optimize()
        {
         
        }

        public void Close()
        {
            _dataStorage.Close();
            _indexManager.Close();
        }

    }
    class DataStorage
    {
        private readonly KDB kDB;
        private readonly FileStream _dataFileWriter;
        private readonly FileStream _dataFileReader;

        public DataStorage(KDB kDB)
        {
            this.kDB = kDB ?? throw new ArgumentNullException(nameof(kDB));

            _dataFileWriter = new FileStream(this.kDB.DBPath + "/data.db", FileMode.Append, FileAccess.Write, FileShare.Read);
            _dataFileReader = new FileStream(this.kDB.DBPath + "/data.db", FileMode.Open, FileAccess.Read, FileShare.Write);
        }

        public DataPointer Put(string key, byte[] valueBytes)
        {
            byte[] data = new byte[64 + key.Length + valueBytes.Length + 1];
            var pos = _dataFileWriter.Length;

            var keyLenBytes = BitConverter.GetBytes(key.Length);
            var valueLenBytes = BitConverter.GetBytes(valueBytes.Length);
            var keyBytes = Encoding.ASCII.GetBytes(key);

            keyLenBytes.CopyTo(data, 0);
            valueLenBytes.CopyTo(data, 33);
            keyBytes.CopyTo(data, 65);
            valueBytes.CopyTo(data, 65 + key.Length);

            _dataFileWriter.Write(data, 0, data.Length);
            _dataFileWriter.Flush();

            return new DataPointer(key, key.Length, valueBytes.Length, pos);
        }

        public byte[] Get(DataPointer dataPointer)
        {
            var metaSize = 64 + dataPointer._keyLen;
            byte[] value = new byte[dataPointer._valueLen + 1];
            byte[] data = new byte[metaSize + dataPointer._valueLen + 1];

            _dataFileReader.Seek(dataPointer._postion, SeekOrigin.Begin);
            _dataFileReader.Read(data, 0, data.Length);

            for (int i = metaSize + 1, j = 0; i < data.Length; i++, j++)
            {
                value[j] = data[i];
            }

            return value;
        }

        public void Close()
        {
            _dataFileReader.Close();
            _dataFileWriter.Close();
        }
    }
    class IndexStorage
    {
        private readonly KDB _kDB;

        public IndexStorage(KDB kDB)
        {
            _kDB = kDB ?? throw new ArgumentNullException(nameof(kDB));
        }

        public void Persist(MemTable memTable, int fileNumber)
        {
            var rowAt = 0;
            var len = (memTable.Store.Count * (64 + 64 + 64 + 1)) + memTable.TotalKeyLength;
            byte[] data = new byte[len];

            foreach (var item in memTable.Store)
            {
                var keyBytesLength = item.Key.Length;
                byte[] row = new byte[64 + keyBytesLength + 64 + 64];

                var keyBytes = Encoding.ASCII.GetBytes(item.Key);
                var keyLengthBytes = BitConverter.GetBytes((long)keyBytesLength);
                var positionBytes = BitConverter.GetBytes(item.Value._postion);
                var valueLengthBytes = BitConverter.GetBytes(item.Value._valueLen);

                keyLengthBytes.CopyTo(row, 0);
                keyBytes.CopyTo(row, 65);
                positionBytes.CopyTo(row, 64 + keyBytesLength + 1);
                valueLengthBytes.CopyTo(row, 64 + keyBytesLength + 64 + 1);

                row.CopyTo(data, rowAt);

                rowAt += row.Length + 1;
            }

            var indexFile = new FileStream(_kDB.DBPath + "/Index-" + fileNumber.ToString() + ".db", FileMode.CreateNew);
            indexFile.Write(data, 0, len);
            indexFile.Close();
        }
    }
}
