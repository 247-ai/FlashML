package com.tfs.flashml.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Class to register a bunch of classes for Kryo serialization.
  * @since 16/1/18
  */

class FlashMLKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Byte])
    kryo.register(classOf[String])
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(classOf[org.apache.spark.sql.types.NullType])
    kryo.register(classOf[org.apache.spark.sql.types.DateType])
    kryo.register(classOf[org.apache.spark.sql.types.NumericType])
    kryo.register(classOf[org.apache.spark.sql.types.ObjectType])
    kryo.register(classOf[org.apache.spark.sql.types.LongType])
    kryo.register(classOf[org.apache.spark.sql.types.FloatType])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(Class.forName("org.apache.spark.sql.types.StringType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.LongType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.Metadata"))
    kryo.register(Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"))
    kryo.register(Class.forName(("[[B")))
    kryo.register(Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"))
    kryo.register(classOf[Array[Object]])
    kryo.register(classOf[org.apache.spark.unsafe.types.UTF8String])
    kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("scala.collection.mutable.WrappedArray$ofRef"))
    kryo.register(classOf[Array[org.apache.spark.sql.catalyst.InternalRow]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow])
    kryo.register(classOf[org.apache.spark.mllib.stat.MultivariateOnlineSummarizer])
  }
}
