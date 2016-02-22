package com.kunyandata.nlpsuit.deduplication

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/2/18.
  */
object titleDeduplication {

  private def formatTitle(titleString: String): String = {
    /**
      * formatTitle(titleString)
      *
      * args:
      * title: 标题文本字符串
      *
      * return:
      * result: 格式化后的文本标题
      */
    val engPunc = Array(",", ".", "!", ";", ":","\"","\"")
    val chiPunc = Array("，", "。", "！", "；", "：", "“", "”")
    val blank = """\s"""
    var result = titleString.toLowerCase().replaceAll(blank, "")
    for (ind <- chiPunc) {
      result = result.replaceAll(ind, engPunc(chiPunc.indexOf(ind)))
    }
    result
  }

  private def hashList(titleString: String, n: Int) = {
    /**
      * hashList(title, n)
      *
      * args:
      * title: 标题文本字符串
      * n: 字符串窗口大小
      *
      * return:
      * titleList: 标题hashcode List
      */
      val title = formatTitle(titleString)
      val titleList= new ArrayBuffer[Int]
      var loopCtrl = true
      for (w <- title if loopCtrl){
        val indexOfw = title.indexOf(w)
        val indexOfRange = indexOfw + n
        titleList.+=(title.slice(indexOfw, indexOfRange).hashCode)
        loopCtrl = indexOfw < (title.length - n)
      }
      titleList
    }

  def process(titleA: String, titleB: String, n: Int, threshold: Double): Boolean = {
    /**
      * process(titleA, titleB, n, threshold)
      *
      * args:
      * titleA: 文本标题字符串
      * titleB: 文本标题字符串（与titleA分别为两个需要比较的标题，交换顺序不影响结果）
      * n: 字符串窗口大小
      * threshold: 判断是否相似的阈值
      *
      * return:
      * true和false
      */
    val titleListA = hashList(titleA, n)
    val titleListB = hashList(titleB, n)
    val pValue = titleListA.intersect(titleListB).length*1.0/
      Array(titleListA.length, titleListB.length).max
    if (pValue >= threshold) true
    else false
  }
}
