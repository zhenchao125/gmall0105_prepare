package com.atguigu.dw.gmall0105.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long) {
    private val date = new Date(ts)
    // 为了将来方便在es中查询, 添加3种格式的日期
    val logDate: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    val logHour: String = new SimpleDateFormat("HH").format(date)
    val logHourMinute: String = new SimpleDateFormat("HH:mm").format(date)
}
