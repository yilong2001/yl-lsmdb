package com.example.lsm

//import java.util
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import java.util.function.Consumer

import com.example.jutil.Constant

import scala.collection._
import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.runtime.Nothing$

/**
  * Created by yilong on 2018/1/26.
  */
class MetaDataTable {
  //[file, indexblocks, firstkey, lastkey]
  private var dataFileMetaTable = new mutable.HashMap[Int, mutable.ListBuffer[DataFileMeta]]
  private var tsFileMetaTable = new mutable.HashMap[Int, mutable.ListBuffer[TSFileMeta]]

  dataFileMetaTable.put(Constant.LEVEL_1, new mutable.ListBuffer[DataFileMeta]())
  dataFileMetaTable.put(Constant.LEVEL_2, new mutable.ListBuffer[DataFileMeta]())

  tsFileMetaTable.put(Constant.LEVEL_1, new mutable.ListBuffer[TSFileMeta]())
  tsFileMetaTable.put(Constant.LEVEL_2, new mutable.ListBuffer[TSFileMeta]())

  def updateMetaData(addMds : mutable.HashMap[Int, ArrayBuffer[DataFileMeta]],
                     rmMds : mutable.HashMap[Int, ArrayBuffer[DataFileMeta]]) = {

    dataFileMetaTable.synchronized{
    (rmMds != null) match {
      case true =>
        for ((level, mdarr) <- rmMds) {
        var list = dataFileMetaTable.get(level).get

        mdarr.foreach(md => {
          list = list.filter(_ != md)
        })
        dataFileMetaTable.put(level, list)
      }
      case false => {}
    }

    (addMds != null) match {
      case true =>
        for ((level , mdarr) <- addMds) {
        var list = dataFileMetaTable.get(level).get

        mdarr.foreach(md => {
          list.+=:(md)
        })
      }
      case false => {}
    }}
  }

  def addDataFileMeta(level : Int, dfm : DataFileMeta) = {
    dataFileMetaTable.synchronized{
      var list = dataFileMetaTable.get(level).get

      list.append(dfm)
      dataFileMetaTable.put(level, list)
    }
  }

  def removeDataFileMeta(level : Int, dfm : DataFileMeta) = {
    dataFileMetaTable.synchronized{
      var list = dataFileMetaTable.get(level).get

      list = list.filter(m => {m.filename.equals(dfm.filename)})
      dataFileMetaTable.put(level, list)
    }
  }

  def addTsFileMeta(level : Int, dfm : TSFileMeta) = {
    tsFileMetaTable.synchronized{
      var list = tsFileMetaTable.get(level).get

      list.append(dfm)
      tsFileMetaTable.put(level, list)
    }
  }

  def removeTsFileMeta(level : Int, dfm : TSFileMeta) = {
    tsFileMetaTable.synchronized{
      var list = tsFileMetaTable.get(level).get

      list = list.filter(m => {
        !m.filename.equals(dfm.filename)
      })

      tsFileMetaTable.put(level, list)
    }
  }

  def findDataFile(level : Int, key : String) : mutable.ListBuffer[String] = {
    dataFileMetaTable.synchronized{
      val list = dataFileMetaTable.get(level)
      val destMds = list.get.filter(md => {
        ((md.firstKey.compareTo(key) <= 0) && (md.lastKey.compareTo(key) >= 0))
      })

      val out = for (x <- destMds) yield x.filename
      out
    }
  }

  def findTsFile(level : Int, range : TimestampRange) : mutable.ListBuffer[String] = {
    tsFileMetaTable.synchronized{
      val list = tsFileMetaTable.get(level)
      val destMds = list.get.filter(ts => {
        ts.firstTs <= range.max && ts.lastTs >= range.min
      })

      val out = for (x <- destMds) yield x.filename
      out
    }
  }

  def display() = {
    println("------------- dataFileMetaTable begin -----------------------")
    dataFileMetaTable.get(Constant.LEVEL_1).get.foreach(md => {
      println("level_1:"+md.filename)
    })

    dataFileMetaTable.get(Constant.LEVEL_2).get.foreach(md => {
      println("level_2:"+md.filename)
    })
    println("------------- dataFileMetaTable end -----------------------")
  }
}
