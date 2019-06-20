package com.atguigu.dw.gmall0105publisher.service

trait PublisherService {
    /**
      * 获取指定日期的日活总数
      *
      * @param date 指定的日期: 格式 2019-05-15
      * @return 日活总数
      */
    def getDauTotal(date: String): Long
    
    /**
      * 获取指定日期日活的小时统计
      *
      * @param date
      * @return
      */
    def getDauHour2countMap(date: String): Map[String, Long]
}
