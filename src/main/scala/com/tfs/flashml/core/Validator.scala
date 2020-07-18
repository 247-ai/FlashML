package com.tfs.flashml.core

trait Validator
{
    /**
      * @return Validation status.
      */
    def validate(): Unit
}
