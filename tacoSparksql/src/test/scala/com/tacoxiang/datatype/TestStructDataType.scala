package com.tacoxiang.datatype

import org.junit.Test

/**
 * ****************************************************************************************                                                 
 * Programs are meant to be read by humans and only incidentally for computers to execute
 * ****************************************************************************************
 * Author : tacoxiang                          
 * Time : 2020/3/26                            
 * Package : com.tacoxiang.datatype      
 * ProjectName: Bigdata
 * Describe :                                
 * ============================================================
 **/
class TestStructDataType {
  @Test
  def getResourcePath(): Unit = {
    val filePath = this.getClass().getClassLoader().getResource("experiment/people").getPath()
    println(filePath)
  }
  object TestStructDataType
}
