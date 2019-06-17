package com.atguigu.dw.gmall0105logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.dw.gmall0105.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @Author lzc
 * @Date 2019-06-16 16:53
 */
@Controller  // 表示这个类是个Controller
public class LoggerController {

    @ResponseBody // 表示返回值"success"是响应体而不是一个页面
    @PostMapping("/log")  // 表示处理post请求 路径是: /log
    public String doLog(@RequestParam("log") String log) {  // 把传递传递过来的参数log的值赋值给变量log
        // 1. 添加时间戳
        JSONObject logObj = addTS(JSON.parseObject(log));
        //2. 落盘
        saveLog(logObj);
        //3. 写到kafka
        sendToKafka(logObj);
        return "success";
    }


    /**
     * 添加时间戳
     *
     * @param logObj
     * @return
     */
    public JSONObject addTS(JSONObject logObj) {
        logObj.put("ts", System.currentTimeMillis());
        return logObj;
    }

    // 初始化 Logger 对象
    private final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /**
     * 日志落盘
     * 使用 log4j
     *
     * @param logObj
     */
    public void saveLog(JSONObject logObj) {

        logger.info(logObj.toJSONString());
    }

    // 使用注入的方式来实例化 KafkaTemplate. Spring boot 会自动完成
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送日志到 kafka
     *
     * @param logObj
     */
    private void sendToKafka(JSONObject logObj) {
        String logType = logObj.getString("type");
        String topicName = GmallConstant.TOPIC_STARTUP;

        if ("event".equals(logType)) {
            topicName = GmallConstant.TOPIC_EVENT;
        }
        kafkaTemplate.send(topicName, logObj.toJSONString());
    }
}

/**
 * 业务:
 * 1. 给日志添加时间戳 (客户端的时间有可能不准, 所以使用服务器端的时间)
 *
 * 2. 日志落盘
 *      主要是在本地也保存一份
 *
 * 3. 日志发送 kafka
 */

