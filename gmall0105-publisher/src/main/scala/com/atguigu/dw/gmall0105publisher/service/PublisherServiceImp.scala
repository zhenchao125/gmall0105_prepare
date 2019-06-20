package com.atguigu.dw.gmall0105publisher.service

import java.util

import io.searchbox.client.JestClient
import io.searchbox.core.search.aggregation.TermsAggregation
import io.searchbox.core.{Search, SearchResult}
import org.springframework.beans.factory.annotation.Autowired

import scala.collection.mutable

class PublisherServiceImp extends PublisherService {
    @Autowired
    private var jestClient: JestClient = _
    
    /**
      * 获取指定日期的日活总数
      *
      * @param date 指定的日期: 格式 2019-05-15
      * @return 日活总数
      */
    override def getDauTotal(date: String): Long = {
        // 自动注入
        // 1. 定义查询 DSL
        val queryDSL =
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "filter": {
           |        "term": {
           |          "logDate": "$date"
           |        }
           |      }
           |    }
           |  }
           |}
             """.stripMargin
        // 2. 创建 Search 对象
        val search: Search = new Search.Builder(queryDSL)
            .addIndex("gmall0105_dau")
            .addType("_doc").build()
        // 3. 执行查询
        val result: SearchResult = jestClient.execute(search)
        // 4. 返回总数
        result.getTotal.toLong
    }
    
    /**
      * 获取指定日期日活的小时统计
      *
      * @param date
      * @return
      */
    override def getDauHour2countMap(date: String): Map[String, Long] = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "logDate": "$date"
               |        }
               |      }
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_hour": {
               |      "terms": {
               |        "field": "logHour",
               |        "size": 24
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        
        val search = new Search.Builder(queryDSL)
            .addIndex("gamll0105_dau")
            .addType("_doc")
            .build()
        val result: SearchResult = jestClient.execute(search)
        val buckets: util.List[TermsAggregation#Entry] = result.getAggregations.getTermsAggregation("groupby_hour").getBuckets
        
        val hour2countMap: mutable.Map[String, Long] = mutable.Map[String, Long]()
        for (i <- 0 until buckets.size) {
            val bucket: TermsAggregation#Entry = buckets.get(i)
            hour2countMap += bucket.getKey -> bucket.getCount
        }
        hour2countMap.toMap
    }
}
