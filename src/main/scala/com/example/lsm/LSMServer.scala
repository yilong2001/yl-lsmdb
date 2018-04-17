package com.example.lsm

//import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, ConcurrentLinkedQueue, TimeUnit}

import com.example.jutil.{SSConfig, Constant}

import scala.collection.mutable.ArrayBuffer
import scala.collection._

import scala.collection.JavaConversions._

/**
  * Created by yilong on 2018/1/25.
  */

class LSMServer(val config : SSConfig)  extends Runnable  {
  private val immutableMemtableQueue = new ConcurrentLinkedQueue[MemTable]

  private val currentMemtableQueue = new ConcurrentLinkedQueue[MemTable]
  currentMemtableQueue.add(new MemTable)

  private val currentMemtableItemCount = new AtomicInteger(0)

  //private var currentMemtable = new MemTable
  private val dataFileFlusher = new LSMDataFileFlusher(config)
  private val tsFileFlusher = new LSMTSFileFlusher(config)

  private val metaTable = new MetaDataTable
  private val storeFileManager = new LSMFileManager(config)

  private val stopFlag = new AtomicBoolean(false)
  var immutableTableCount = 0;

  override def run(): Unit = {
    while(!stopFlag.get()) {
      println("SSTableServer.run : " + Thread.currentThread().getName)
      try {
        flush()
        cleanTimeoutImmutableMemtable()
      } finally {
        try {
          Thread.sleep(1000)
        } catch {
          case e : InterruptedException => {
            e.printStackTrace()
            throw new RuntimeException(e.getMessage)
          }
        }
      }
    }
  }

  def stop() = {
    stopFlag.set(true)
    //TODO
  }

  def put(k : String, v : String) = {
    if (currentMemtableItemCount.incrementAndGet() == Constant.MEM_TABLE_NON_LOCK_SIZE_THRESHOLD) {
      currentMemtableQueue.synchronized({
        currentMemtableQueue.add(new MemTable)
        currentMemtableItemCount.set(0)
        val item = currentMemtableQueue.poll()
        immutableMemtableQueue.add(item)
      })
    }

    val peek = currentMemtableQueue.peek()
    peek.put(k,v)

    println("SSTableServer.put : "+k+":"+v)
  }

  private def flush() = {
    println("SSTableServer.flush : "+Thread.currentThread().getName)
    immutableMemtableQueue.foreach(mt => {
      if (!mt.isFlushed()) {
        val ts = System.currentTimeMillis()
        val datafilename = config.getLevel0DataFilename(ts)
        val tsfilename = config.getLevel0TSFilename(ts)

        val dfm = dataFileFlusher.flush(datafilename, mt)
        val tsfm = tsFileFlusher.flush(tsfilename, mt)

        metaTable.addDataFileMeta(Constant.LEVEL_1, dfm)
        metaTable.addTsFileMeta(Constant.LEVEL_1, tsfm)

        mt.setFlushed
      }
    })
  }

  private def cleanTimeoutImmutableMemtable() = {
    val curTs = System.currentTimeMillis()
    immutableMemtableQueue.foreach(mt => {
      if (mt.isFlushed() &&
        curTs - mt.getFlushTime() > Constant.MEM_TABLE_FLUSH_INTO_FILE_KEEP_TIME) {
        immutableMemtableQueue.remove(mt)
        //TODO: mt mem should release
      }
    })
  }

  private def findCurrentMemtable(k : String) : mutable.ListBuffer[TimestampValue] = {
    //var v : java.util.List[String] = new java.util.ArrayList[String]()
    var v = new mutable.ListBuffer[TimestampValue]()

    try {
      val peek = currentMemtableQueue.peek()
      val v1 = peek.find(k)
      if (v1 != null) {
        v.append(v1)
      }
    } catch {
      case e : InterruptedException => e.printStackTrace()
    } finally {

    }

    v
  }

  private def findImmutableMemtable(k : String) : mutable.ListBuffer[TimestampValue] = {
    //var v : java.util.List[String] = new java.util.ArrayList[String]()
    var v = new mutable.ListBuffer[TimestampValue]()

    try {
      immutableMemtableQueue.foreach(mt => {
        val v1 = mt.find(k)
        if (v1 != null) {
          v.append(v1)
        }
      })
    } catch {
      case e : InterruptedException => e.printStackTrace()
    } finally {

    }

    v
  }

  def find(k : String) : immutable.List[TimestampValue] = {
    var v = new mutable.ListBuffer[TimestampValue]()

    v = findCurrentMemtable(k)
    println("SSTableServer.find : findCurrentMemtable( size = " + k + ":" + v.size + " )")

    v = (v.size > 0) match {
      case true => v
      case _ => findImmutableMemtable(k)
    }
    println("SSTableServer.find : findImmutableMemtable( size = " + k + ":" + v.size + " )")

    v = (v.size > 0) match {
      case true => v
      case _ => storeFileManager.find(k, metaTable)
    }
    println("SSTableServer.find : storeFileManager.find( found=" + (v.size > 0) + ": k=" + k + " )")

    v.toList
  }

  private def findCurrentMemtableByTimeRange(mtIdList: mutable.ListBuffer[String],range: TimestampRange) : immutable.TreeMap[String, TimestampValue] = {
    //var v : java.util.List[String] = new java.util.ArrayList[String]()
    var v = new immutable.TreeMap[String, TimestampValue]()

    try {
      val mt = currentMemtableQueue.peek()
      v = mt.findByTimeRange(range)
      mtIdList.append(mt.getId())
    } catch {
      case e : InterruptedException => e.printStackTrace()
    } finally {

    }

    v
  }

  private def findImmutableMemtableByTimeRange(mtIdList: mutable.ListBuffer[String], range: TimestampRange) : immutable.TreeMap[String, TimestampValue] = {
    //var v : java.util.List[String] = new java.util.ArrayList[String]()
    var vlist = new mutable.ListBuffer[immutable.TreeMap[String, TimestampValue]]

    try {
      immutableMemtableQueue.foreach(mt => {
        val v1 = mt.findByTimeRange(range)
        mtIdList.append(mt.getId())
        vlist.append(v1)
      })
    } catch {
      case e : InterruptedException => e.printStackTrace()
    } finally {

    }

    var vo = new immutable.TreeMap[String, TimestampValue]
    vlist.foreach(o => { vo = vo ++ o})

    vo
  }

  //def findByTimeRange(range: TimestampRange): immutable.List[TimestampValue] = {
  //  var mtIdList = new mutable.ListBuffer[String]

  //}
}

object LSMServer {
  def build(config : SSConfig) : LSMServer = {
    val server = new LSMServer(config)
    server
  }
}
