package com.atguigu.dw.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import com.atguigu.dw.gmall0105.realtime.bean.StartupLog
import com.atguigu.dw.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
        val ssc = new StreamingContext(conf, Seconds(5))
        val sourceStream: InputDStream[(String, String)] =
            MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)
        
        // 1. 调整数据结构
        val starupLogDSteam = sourceStream.map {
            case (_, log) => JSON.parseObject(log, classOf[StartupLog])
        }
        // 2. 保存到 redis
        starupLogDSteam.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                it.foreach(startupLog => {
                    // 存入到 Redis value 类型 set, 存储 uid
                    val key = "dau:" + startupLog.logDate
                    client.sadd(key, startupLog.uid)
                })
                client.close()
            })
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
