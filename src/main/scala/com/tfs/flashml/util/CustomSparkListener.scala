package com.tfs.flashml.util

import java.net.ConnectException

import com.tfs.flashml.FlashML
import com.tfs.flashml.util.conf.FlashMLConstants
import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.{DefaultHttpClient, HttpClientBuilder}
import org.apache.http.entity.StringEntity

/**
  * Logs the completion of the spark application and flushes the Printstream holding
  * the errors if any.
  * On appStart it will send running status and on appEnd it will send completed status to flashml api server
  * @since 19/7/18
  */

class CustomSparkListener extends SparkListener {

  private val log = LoggerFactory.getLogger(getClass)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    log.info(s"Application Start at ${applicationStart.time}")
    val projectId = FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)
    val modelId = FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)
    val jobId = FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)
    val status = FlashMLConstants.RUNNING

    val url = FlashMLConfig.getString(FlashMLConstants.APOLLO_API_URL)

    val post = new HttpPost(url)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(s"""{"projectId":"${projectId}","modelId":"${modelId}","status":"${status}"}"""))

    val client = HttpClientBuilder.create().build()

    // send the post request
    try {
      val response = client.execute(post)
    }
    catch{
      case ex:Exception => log.info("Ignoring Exception - Apollo server is not active")
    }
    //FlashML.log4jPR.close()
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info(s"Application Completed at ${applicationEnd.time}")
    val projectId = FlashMLConfig.getString(FlashMLConstants.FLASHML_PROJECT_ID)
    val modelId = FlashMLConfig.getString(FlashMLConstants.FLASHML_MODEL_ID)
    val jobId = FlashMLConfig.getString(FlashMLConstants.FLASHML_JOB_ID)
    val status = FlashMLConstants.COMPLETED

    val url = FlashMLConfig.getString(FlashMLConstants.APOLLO_API_URL)

    val post = new HttpPost(url)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(s"""{"projectId":"${projectId}","modelId":"${modelId}","status":"${status}"}"""))

    val client = HttpClientBuilder.create().build()

    // send the post request
    try {
      val response = client.execute(post)
    }
    catch{
      case ex:Exception => log.info("Ignoring Exception - Apollo server is not active")
    }

   // FlashML.log4jPR.close()
  }
}
