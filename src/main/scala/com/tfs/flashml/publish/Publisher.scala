package com.tfs.flashml.publish

/**
  * Trait for setting up Publisher objects
  */
trait Publisher
{
    /**
      * Method to generate the JS code for each page.
      * @param pageNumber
      * @return
      */
    def generateJS(pageNumber : Int): StringBuilder

}
