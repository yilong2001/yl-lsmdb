package com.example.sutil

import scala.collection.mutable

/**
  * Created by yilong on 2018/2/25.
  */
abstract class IBlock {
  def save(data : Array[Byte]) : Boolean
  def avaliableSize() : Int
  def getBufArray() : mutable.ListBuffer[Array[Byte]]
  def clean() : Unit
}
