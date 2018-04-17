package com.example.lsm

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.example.jutil.{SSConfig, Constant}

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yilong on 2018/1/31.
  */
class LSMFileManager(config : SSConfig) {
  private val dataFileIndexBlocks = new ConcurrentHashMap[String, immutable.List[DataBlockIndexOffset]]()

  private val dataFileKVBlocks = new ConcurrentHashMap[String, immutable.Map[String, TimestampValue]]()

  private val dataFileReader = new LSMDataFileReader(config)

  private val tsFileReader = new LSMTSFileReader(config)

  def findBlockOffset(k:String, filename:String) : Int = {
    var res = -1
    if (!dataFileIndexBlocks.contains(filename)) {
      //TODO: should sync for readIndex operation
      val indexOffs = dataFileReader.readIndexBlocks(filename)
      if (indexOffs.size > 0) {
        dataFileIndexBlocks.putIfAbsent(filename, indexOffs)
      } else {
        throw new RuntimeException(filename+" read index failed!")
      }
    }

    val indexOffs = dataFileIndexBlocks.get(filename)
    println("SSFileManager.findBlockOffset: filename="+filename+"; kvmap.size="+indexOffs.size)

    if (indexOffs != null) {
      scala.util.control.Breaks.breakable {
        indexOffs.foreach( off => {
          if (k.compareTo(off.firstKey) >= 0 && k.compareTo(off.lastKey) <= 0) {
            res = off.offset
            scala.util.control.Breaks.break
          }
        })
      }

      println("SSFileManager.findBlockOffset: found off = " + res)
    }

    res
  }

  def queryBlock(k:String, filename: String, blockOffset:Int) : TimestampValue = {
    val nk = (filename + "___" + blockOffset)
    var kvmap = dataFileKVBlocks.get(nk)
    if (kvmap == null) {
      //TODO: should sync for readblock operation
      kvmap = dataFileReader.readDataBlock(filename, blockOffset)
      if (kvmap.size > 0) {
        dataFileKVBlocks.putIfAbsent(nk, kvmap)
      } else {
        throw new RuntimeException(nk+", no keyvalue")
      }

      println("SSFileManager.queryBlock: k="+k+"; nk="+nk+"; kvmap.size="+kvmap.size)
    }

    (kvmap == null || kvmap.size == 0) match {
      case true => null
      case false => {
        kvmap.get(k).getOrElse(null)
      }
    }
  }

  def find(k : String, metaTable: MetaDataTable) : mutable.ListBuffer[TimestampValue] = {
    //metaTable.display()
    //1, query offset in ssfiles from metadat
    var out = new mutable.ListBuffer[TimestampValue]

    var ssfilenames : ListBuffer[String] = new ListBuffer[String]
    List(Constant.LEVEL_1, Constant.LEVEL_2).foreach(l => {
      val ssfile = metaTable.findDataFile(l, k)
      if (ssfile != null && ssfile.size > 0) {
        ssfilenames = ssfilenames ++ (ssfile)
      }
    })

    println("--------------------------- SSFileManager.find begin ---------------------------------")
    scala.util.control.Breaks.breakable {
      ssfilenames.foreach(f => {
        val off = findBlockOffset(k, f)
        if (off >= 0) {
          val value = queryBlock(k, f, off)
          if (value != null) {
            out.append(value)
            scala.util.control.Breaks.break
          }
        }
      })
    }

    println("--------------------------- SSFileManager.find end ---------------------------------")

    out
  }

  def findByTimeRange(range:TimestampRange, meta: MetaDataTable) : immutable.TreeMap[String, Option[TimestampValue]] = {
    var ssfilenames : ListBuffer[String] = new ListBuffer[String]
    List(Constant.LEVEL_1, Constant.LEVEL_2).foreach(l => {
      val ssfile = meta.findTsFile(l, range)
      if (ssfile != null && ssfile.size > 0) {
        ssfilenames = ssfilenames ++ (ssfile)
      }
    })

    val keylist = new ListBuffer[String]
    ssfilenames.foreach(f => {
      keylist.++=:(tsFileReader.find(range, f))
    })

    val v = new mutable.HashMap[String, Option[TimestampValue]]()

    keylist.foreach(k => {
      val tsv = find(k, meta)
      val one = tsv.size > 0 match {
        case true => Option.apply(tsv.apply(0))
        case false => Option.apply(null)
      }
      v.put(k, one)})

    val vo = new immutable.TreeMap[String, Option[TimestampValue]]

    vo ++ v
  }

  def printMeta() = {
    println()
  }
}
