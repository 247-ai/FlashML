package com.tfs.flashml

import java.nio.file.{Files, Paths}

object TestUtils
{
    /**
      * Get the location of the data folder.
      * When run in IDE, the folder is located at <root>/flashml-core/, but
      * when run from docker, it is located at <root>.
      */
    def getDataFolder = getValidFolder(Array("data", "flashml-core/data"))

    /**
      * Get the location of the support files.
      * When run in IDE, the folder is located at <root>/flashml-core/, but
      * when run from docker, it is located at <root>.
      */
    def getSupportFileFolder = getValidFolder(Array("support_files", "flashml-core/support_files"))

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
