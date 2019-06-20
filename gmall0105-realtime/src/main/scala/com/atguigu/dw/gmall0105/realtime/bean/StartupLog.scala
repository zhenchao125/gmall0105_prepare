package com.atguigu.dw.gmall0105.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      `type`: String,
                      version: String,
                      ts: Long) {
    private val date = new Date(ts)
    val logDate: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    val logHour: String = new SimpleDateFormat("HH").format(date)
    val logHourMinute: String = new SimpleDateFormat("HH:mm").format(date)
}
