package com.example.lsm

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.example.sutil.CmpUtils

import scala.collection.{immutable, mutable}
import scala.util.Random
import scala.collection.JavaConversions._

/**
  * Created by yilong on 2018/1/25.
  */
class MemTable {
  private val flushed = new AtomicBoolean(false)
  private var flushTime = 0L
  private var curMemTable = new ConcurrentSkipListMap[String, TimestampValue]
  private val id = System.currentTimeMillis()+"-"+(new Random(System.currentTimeMillis()).nextInt() % 100000)

  def put(k : String, v : String) : Boolean = {
    //TODO: 并发情况下, 可能会出现immutuable时, totalCount > Constant.MEM_TABLE_NON_LOCK_SIZE_THRESHOLD
    val tsv = TimestampValue(System.currentTimeMillis(), v)
    curMemTable.put(k, tsv)
    return true
  }

  def put(k : String, v : String, ts: Long) : Boolean = {
    //TODO: 并发情况下, 可能会出现immutuable时, totalCount > Constant.MEM_TABLE_NON_LOCK_SIZE_THRESHOLD
    val tsv = TimestampValue(ts, v)
    curMemTable.put(k, tsv)
    return true
  }

  def setFlushed = {
    flushed.compareAndSet(false, true)
    flushTime = System.currentTimeMillis()
  }

  def isFlushed() : Boolean = {
    return flushed.get()
  }

  def find(k : String) : TimestampValue = {
    curMemTable.get(k)
  }

  def findByTimeRange(range: TimestampRange) : immutable.TreeMap[String, TimestampValue] = {
    var v = new mutable.HashMap[String, TimestampValue]()

    for ((k, tsv) <- curMemTable) {
      if (CmpUtils.isTsSatisfied(tsv.ts, range)) {
        v.put(k, tsv)
      }
    }

    val vx = new immutable.TreeMap[String, TimestampValue]
    vx ++ (v)
  }

  def release() = {
    curMemTable.clear()
  }

  def getMemtable() = {
    curMemTable
  }

  def getFlushTime() : Long = {
    flushTime
  }

  def getId() : String = {
    id
  }
}
