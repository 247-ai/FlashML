package com.tfs.flashml

import java.nio.file.{Files, Paths}

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
}
