package com.tfs.flashml

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object TestUtils
{
    /**
      * Get the location of the data folder.
      */
    def getDataFolder = getValidFolder(Array("data", "data"))

    /**
      * Get the location of the support files.
      */
    def getSupportFileFolder = getValidFolder(Array("support_files", "support_files"))

    private def getValidFolder(folders: Array[String]) =
    {
        // Look through a series of locations
        folders.find(v => Files.exists(Paths.get(v))) match
        {
            case Some(x) => x
            case None => throw new RuntimeException(s"Unable to find data folder in [${folders.mkString(":")}]")
        }
    }

    /**
      * UDF for obtaining the positive probability column
      */
    val pos_prob: UserDefinedFunction = udf((a: DenseVector) => a(1))
}
