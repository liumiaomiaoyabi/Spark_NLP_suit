package com.kunyandata.nlpsuit.deduplication

import scala.collection.mutable.ArrayBuffer

/**
  * Created by QQ on 2016/2/18.
  */
object abcd {

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

  /**
    * 对两个title字符串进行对比，返回是否疑似重复.
    * @param titleStringA 文本标题字符串（与titleStringB分别为两个需要比较的标题，交换顺序不影响结果）
    * @param titleStringB 文本标题字符串（与titleStringA分别为两个需要比较的标题，交换顺序不影响结果）
    * @param n 字符串窗口大小，建议设置2-4
    * @param threshold 判断是否相似的阈值，一般设置0.5-0.8（这个需要实际的检验）
    * @return 返回值为布尔值，true代表两个标题疑似重复，false反之
    */
  def process(titleStringA: String, titleStringB: String, n: Int, threshold: Double): Boolean = {
    /**
      * process(titleStringA, titleStringB, n, threshold)
      *
      * args:
      * titleStringA: 文本标题字符串
      * titleStringB: 文本标题字符串（与titleStringA分别为两个需要比较的标题，交换顺序不影响结果）
      * n: 字符串窗口大小
      * threshold: 判断是否相似的阈值
      *
      * return:
      * true和false
      */
    val titleListA = hashList(titleStringA, n)
    val titleListB = hashList(titleStringB, n)
    val pValue = titleListA.intersect(titleListB).length*1.0/
      Array(titleListA.length, titleListB.length).max
    if (pValue >= threshold) true
    else false
  }
}