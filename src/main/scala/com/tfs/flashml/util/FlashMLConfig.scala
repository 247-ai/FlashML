package com.tfs.flashml.util

import java.io.File
import java.net.URI

import com.tfs.flashml.core.DirectoryCreator
import com.tfs.flashml.dal.SavePointManager
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object FlashMLConfig
{
    private val log = LoggerFactory.getLogger(getClass)
    private val masterConfPath = getClass.getResource("/MasterConfig.json")
    private val longList: Config = ConfigFactory.parseFile(new File(masterConfPath.getPath))

    /**
      * Method for loading configurations from HDFS
      *
      * @param pathToConf
      * @return
      */
    def loadConfFromHDFS(pathToConf: String): Config =
    {
        val path = new Path(pathToConf)
        val confFile = File.createTempFile("localConfig", ".json")
        confFile.deleteOnExit()
        getFileSystemByUri(path.toUri).copyToLocalFile(path, new Path(confFile.getAbsolutePath))
        ConfigFactory.parseFile(confFile)
    }

    def hasKey(key: String): Boolean =
    {
        config.hasPath(key)
    }

    def getFileSystemByUri(uri: URI): FileSystem =
    {
        val hdfsConf = new Configuration()
        hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
        hdfsConf.set("fs.parameter.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
        FileSystem.get(uri, hdfsConf)
    }

    /**
      * The configurations read from the FlashML config file.
      */
    var config: Config = if (ConfigValues.configFilePath.startsWith("hdfs"))
    {
        loadConfFromHDFS(ConfigValues.configFilePath).withFallback(longList)
    }
    else
    {
        val configFile = new File(ConfigValues.configFilePath)
        // Issue a warning if the config file is not found.
        if (!ConfigValues.configFilePath.isEmpty && !configFile.exists())
            log.warn(s"Config file doesn't exist: [${configFile.getAbsolutePath}]. Using fallback configurations.")
        ConfigFactory.parseFile(configFile).withFallback(longList)
    }

    /**
      * Method to add a few configuration settings to an existing configuration, mostly for unit testing purposes.
      *
      * @param map
      */
    def addToConfig(map: Map[String, String]) =
    {
        val newConfig = ConfigFactory.parseMap(map.asJava)
        config = newConfig.withFallback(config)
    }

    // Utility methods for retrieving various values from the config
    /**
      * @param key The string value read
      * @return Array of Hashmaps
      */
    def getListHashMaps(key: String): Array[mutable.Map[String, Any]] = config
            .getList(key)
            .asScala
            .map(_
                    .unwrapped()
                    .asInstanceOf[java.util.HashMap[String, Any]]
                    .asScala
            )
            .toArray

    // Utility methods for retrieving various values from the config
    /**
      * @param key The string value read
      * @return Two dimensional Array of Hashmaps
      */
    def get2DListHashMaps(key: String): Array[Array[mutable.Map[String, Any]]] = config
            .getList(key)
            .asScala
            .map(_
                    .unwrapped()
                    .asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                    .asScala
                    .map(_.asScala)
                    .toArray)
            .toArray


    def getString(key: String): String = if (config.hasPath(key)) config.getString(key) else ""

    def getInt(key: String): Int = if (config.hasPath(key)) config.getInt(key)
    else Int.MinValue

    def get2DStringArray(key: String): Array[Array[String]] =
    {
        config
            .getList(key)
            .asScala
            .map(_.unwrapped()
                    .asInstanceOf[java.util.ArrayList[String]]
                    .asScala
                    .map(_.toString)
                    .toArray)
            .toArray
    }

    def get2DArrayInt(key: String): Array[Array[Int]] =
    {
        config
            .getList(key)
            .asScala
            .map(_
                    .unwrapped()
                    .asInstanceOf[java.util.ArrayList[Int]]
                    .asScala
                    .toArray)
            .toArray
    }

    def getStringArray(key: String): Array[String] =
    {
        config
            .getStringList(key)
            .asScala
            .toArray
    }

    def getIntArray(key: String): Array[Int] =
    {
        config.getIntList(key).asScala.map(_.toInt).toArray
    }

    def getDoubleArray(key: String): Array[Double] =
    {
        config.getDoubleList(key).asScala.map(_.toDouble).toArray
    }

    def getDouble(key: String): Double = config.getDouble(key)

    def getBool(key: String): Boolean = if(config.hasPath(key)) config.getBoolean(key) else false

    def getBoolArray(key: String): Array[Boolean] =
    {
        config.getBooleanList(key).asScala.map(_.toString.toBoolean).toArray
    }
}