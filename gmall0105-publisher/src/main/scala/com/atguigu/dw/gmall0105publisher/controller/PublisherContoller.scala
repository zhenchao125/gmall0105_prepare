package com.atguigu.dw.gmall0105publisher.controller

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.dw.gmall0105publisher.service.{PublisherService, PublisherServiceImp}
import org.apache.commons.lang.time.DateUtils
import org.json4s.jackson.JsonMethods
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestParam, RestController}

import scala.collection.mutable


/*
日活:
    http://hadoop201:8070/realtime-total?date=2019-05-15
    
    [
        {"id":"dau","name":"新增日活","value":1200},
        {"id":"new_mid","name":"新增用户","value":233}
    ]
*/
/*
明细:
    http://hadoop201:8070/realtime-hour?id=dau&&date=2019-05-15
    
    {
        "yesterday":{"钟点":数量, "钟点":数量, ...},
        "today":{"钟点":数量, "钟点":数量, ...}
    }
*/

@RestController
class PublisherController {
    @Autowired
    var publisherService: PublisherService = _
    
    @GetMapping(Array("realtime-total"))
    def getRealTimeTotal(@RequestParam("date") date: String): String = {
        val total: Long = publisherService.getDauTotal(date)
        
        val result =
            s"""
               |[
               |    {"id":"dau","name":"新增日活","value":$total},
               |    {"id":"new_mid","name":"新增用户","value":233}
               |]
         """.stripMargin
        result
    }
    
    @GetMapping(Array("realtime-hour"))
    def getRealTimeHour(@RequestParam("id") id: String, @RequestParam("date") date: String) = {
        if (id == "dau") {
            val todayMap: Map[String, Long] = publisherService.getDauHour2countMap(date)
            val yesterdayMap: Map[String, Long] = publisherService.getDauHour2countMap(date2Yesterday(date))
            
            val resultMap: mutable.Map[String, Map[String, Long]] = mutable.Map[String, Map[String, Long]]()
            resultMap += "today" -> todayMap
            resultMap += "yesterday" -> yesterdayMap
            println(resultMap)
            // 转变成 json 格式字符串输出
            import org.json4s.JsonDSL._
            JsonMethods.compact(JsonMethods.render(resultMap))
        } else {
            null
        }
    }
    
    private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    
    // 通过今天计算出来昨天
    private def date2Yesterday(date: String): String = {
        val today: Date = dateFormat.parse(date)
        val yesterday: Date = DateUtils.addDays(today, -1)
        dateFormat.format(yesterday)
    }
}
