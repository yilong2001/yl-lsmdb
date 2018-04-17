import java.io._
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}
import java.util

import com.example.jutil.{Constant, DataTypeTransfer, SSConfig, TestSerObject}
import com.example.lsm._
import com.example.srpc.nettyrpc.serde.{ByteBufferOutputStream}
import io.netty.buffer.ByteBuf
import org.junit.{Assert, Test}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by yilong on 2018/2/1.
  */
@Test
class test extends Assert {

  @Test
  def test_metatable() : Unit = {
    val metatable = new MetaDataTable

    var add = new mutable.HashMap[Int, ArrayBuffer[DataFileMeta]]
    val addArr = new ArrayBuffer[DataFileMeta]
    val md1 = new DataFileMeta("f1", 1, "a1", "a5", "f1")
    val md2 = new DataFileMeta("f2", 1, "b1", "b5", "f2")
    val md3 = new DataFileMeta("f3", 1, "c1", "c5", "f3")
    addArr.append(md1)
    addArr.append(md2)
    addArr.append(md3)

    add.put(1, addArr)

    metatable.updateMetaData(add, null)

    assert("f1" == metatable.findDataFile(1, "a2").apply(0))
    assert("f2" == metatable.findDataFile(1, "b2").apply(0))

    var rm = new mutable.HashMap[Int, ArrayBuffer[DataFileMeta]]
    val rmArr = new ArrayBuffer[DataFileMeta]
    rmArr.append(md1)

    rm.put(1, rmArr)

    metatable.updateMetaData(null, rm)

    val res=metatable.findDataFile(1, "a2")
    assert(0 == res.size)
    assert("f2" == (metatable.findDataFile(1, "b2").apply(0)))
  }

  def test_metatable_2() : Unit = {
    val metatable = new MetaDataTable

    var add = new mutable.HashMap[Int, ArrayBuffer[DataFileMeta]]
    val addArr = new ArrayBuffer[DataFileMeta]
    val md1 = new DataFileMeta("f1", 1, "a1", "a5", "f1")
    val md2 = new DataFileMeta("f2", 1, "b1", "b5", "f2")
    val md3 = new DataFileMeta("f3", 1, "c1", "c5", "f3")

    metatable.addDataFileMeta(1, md1)
    metatable.addDataFileMeta(1, md2)
    metatable.addDataFileMeta(1, md3)

    assert("f1" == metatable.findDataFile(1, "a2").apply(0))
    assert("f2" == metatable.findDataFile(1, "b2").apply(0))

    metatable.removeDataFileMeta(1, md1)

    val res=metatable.findDataFile(1, "a2")
    assert(0 == res.size)
    assert("f2" == (metatable.findDataFile(1, "b2").apply(0)))
  }

  @Test
  def test_ssdfile_write_read(): Unit = {
    val ssfileman = new LSMFileManager(new SSConfig)
    val ssfileflusher = new LSMDataFileFlusher(new SSConfig)

    val memtable = new MemTable

    val treemap = new util.TreeMap[String, String]()
    val rd = new Random()
    for (i <- 0 to 300) {
      val n = rd.nextInt(10000)
      val k = "key_"+n
      val v = "value_"+n
      memtable.put(k, v)
    }

    val filename = "./"+System.currentTimeMillis() + ".ssd"
    val md = ssfileflusher.flush(filename, memtable)

    println(md)

    val it = memtable.getMemtable().entrySet().iterator()
    while(it.hasNext) {
      val entry = it.next()
      var off = ssfileman.findBlockOffset(entry.getKey, md.filename)
      val value = ssfileman.queryBlock(entry.getKey, md.filename, off)
      println(off, value)
      assert(value.value == entry.getValue.value)
    }

    Files.delete(Paths.get(md.filename))
  }

  @Test
  def test_ssfile_read(): Unit = {
    val ssfileman = new LSMFileManager(new SSConfig)

    val key = "key_1010"

    import java.net.URL
    val loader = Thread.currentThread.getContextClassLoader
    val datafile = "test.sstx"

    val url = loader.getResource(datafile)
    val file = url.getPath

    var off = ssfileman.findBlockOffset(key, file)
    println("off : "+off)

    val value = ssfileman.queryBlock(key, file, off)
    assert(value.value == ("value_1010"))
  }

  @Test
  def test_long_serd(): Unit = {
    val ts = System.currentTimeMillis()

    val tsbuf = java.nio.ByteBuffer.allocate(Constant.LONG_SIZE)
    tsbuf.putLong(ts)

    val nts = java.nio.ByteBuffer.wrap(tsbuf.array()).getLong

    assert(ts==nts)
  }

  @Test
  def test_ts_write_read():Unit = {
    //val ssfileman = new LSMFileManager(new SSConfig)
    val ssfileflusher = new LSMTSFileFlusher(new SSConfig)
    val ssfilereader = new LSMTSFileReader(new SSConfig)

    val memtable = new MemTable

    val treemap = new util.TreeMap[String, String]()
    val rd = new Random()

    var list = new ListBuffer[String]
    for (i <- 0 to 300) {
      val n = rd.nextInt(10000)
      val k = "key_"+n
      val v = "value_"+n

      list.append(k)

      memtable.put(k, v, i)
    }

    val filename = "./"+System.currentTimeMillis() + ".sst"
    val md = ssfileflusher.flush(filename, memtable)

    println(md)

    val range = TimestampRange(100, 200)

    val keylist = ssfilereader.find(range, filename)

    for (i <- 100 to 200) {
      assert(keylist.contains(list.apply(i)))
    }

    Files.delete(Paths.get(md.filename))
  }

  @Test
  def test_ts_write_read_byfile(): Unit = {
    val ssdfileflusher = new LSMDataFileFlusher(new SSConfig)

    //val ssfileman = new LSMFileManager(new SSConfig)
    val ssfileflusher = new LSMTSFileFlusher(new SSConfig)
    val ssfilereader = new LSMTSFileReader(new SSConfig)
    val metaTable = new MetaDataTable

    val lsmFile = new LSMFileManager(new SSConfig)

    val memtable = new MemTable

    val treemap = new util.TreeMap[String, String]()
    val rd = new Random()

    var list = new ListBuffer[String]
    for (i <- 0 to 300) {
      val n = rd.nextInt(10000)
      val k = "key_"+n
      val v = "value_"+n

      list.append(k)

      memtable.put(k, v, i)
    }

    val filenamed = "./"+System.currentTimeMillis() + ".ssd"
    val mdd = ssdfileflusher.flush(filenamed, memtable)
    metaTable.addDataFileMeta(1, mdd)

    val filenamet = "./"+System.currentTimeMillis() + ".sst"
    val mdt = ssfileflusher.flush(filenamet, memtable)

    metaTable.addTsFileMeta(1, mdt)

    val range = TimestampRange(100, 200)

    val out = lsmFile.findByTimeRange(range, metaTable)

    println(out.size)

    for( (k,v) <- out ) {
      println(k, v)
    }

    Files.delete(Paths.get(mdd.filename))
    Files.delete(Paths.get(mdt.filename))
  }


//[T: Serializable]
  def serialize2(t: java.io.Serializable): ByteBuffer = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(t)
    out.flush()
    out.close()
    ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
  }

  def deserialize[T : ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteArrayInputStream(bytes.array)
    val in = new ObjectInputStream(bis)
    val t = in.readObject.asInstanceOf[T]
    in.close()
    t
  }
}
