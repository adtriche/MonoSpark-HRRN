/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2014 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import java.io._
import java.util.Properties
import javax.annotation.Nullable

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.AbstractSerDe
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory,
  StructObjectInspector}

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression,
  GenericRow, InterpretedProjection, NamedExpression, Projection, Row, SpecificMutableRow}
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors}
import org.apache.spark.sql.hive.HiveShim.prepareWritable
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{RedirectThread, Utils}

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
private[hive]
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan,
    ioschema: HiveScriptIOSchema)(@transient sc: HiveContext)
  extends UnaryNode {

  override def otherCopyArgs: Seq[HiveContext] = sc :: Nil

  def execute(): RDD[Row] = {
    def processIterator(inputIterator: Iterator[Row]): Iterator[Row] = {
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)

      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val errorStream = proc.getErrorStream

      // In order to avoid deadlocks, we need to consume the error output of the child process.
      // To avoid issues caused by large error output, we use a circular buffer to limit the amount
      // of error output that we retain. See SPARK-7862 for more discussion of the deadlock / hang
      // that motivates this.
      val stderrBuffer = new CircularBuffer(2048)
      new RedirectThread(
        errorStream, stderrBuffer, "Thread-ScriptTransformation-STDERR-Consumer").start()

      val outputProjection = new InterpretedProjection(input, child.output)

      // This nullability is a performance optimization in order to avoid an Option.foreach() call
      // inside of a loop
      @Nullable val (inputSerde, inputSoi) = ioschema.initInputSerDe(input)

      // This new thread will consume the ScriptTransformation's input rows and write them to the
      // external process. That process's output will be read by this current thread.
      val writerThread = new ScriptTransformationWriterThread(
        inputIterator,
        outputProjection,
        inputSerde,
        inputSoi,
        ioschema,
        outputStream,
        proc,
        stderrBuffer,
        TaskContext.get()
      )

      // This nullability is a performance optimization in order to avoid an Option.foreach() call
      // inside of a loop
      @Nullable val (outputSerde, outputSoi) = ioschema.initOutputSerDe(output)

      val reader = new BufferedReader(new InputStreamReader(inputStream))
      val outputIterator: Iterator[Row] = new Iterator[Row] with HiveInspectors {
        var cacheRow: Row = null
        var curLine: String = null
        var eof: Boolean = false

        override def hasNext: Boolean = {
          if (outputSerde == null) {
            if (curLine == null) {
              curLine = reader.readLine()
              if (curLine == null) {
                if (writerThread.exception.isDefined) {
                  throw writerThread.exception.get
                }
                false
              } else {
                true
              }
            } else {
              true
            }
          } else {
            if (eof) {
              if (writerThread.exception.isDefined) {
                throw writerThread.exception.get
              }
              false
            } else {
              true
            }
          }
        }

        def deserialize(): Row = {
          if (cacheRow != null) return cacheRow

          val mutableRow = new SpecificMutableRow(output.map(_.dataType))
          try {
            val dataInputStream = new DataInputStream(inputStream)
            val writable = outputSerde.getSerializedClass().newInstance()
            writable.readFields(dataInputStream)

            val raw = outputSerde.deserialize(writable)
            val dataList = outputSoi.getStructFieldsDataAsList(raw)
            val fieldList = outputSoi.getAllStructFieldRefs()

            var i = 0
            dataList.foreach( element => {
              if (element == null) {
                mutableRow.setNullAt(i)
              } else {
                mutableRow(i) = unwrap(element, fieldList(i).getFieldObjectInspector)
              }
              i += 1
            })
            mutableRow
          } catch {
            case e: EOFException =>
              eof = true
              null
          }
        }

        override def next(): Row = {
          if (!hasNext) {
            throw new NoSuchElementException
          }

          if (outputSerde == null) {
            val prevLine = curLine
            curLine = reader.readLine()
            if (!ioschema.schemaLess) {
              new GenericRow(prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"))
                .asInstanceOf[Array[Any]])
            } else {
              new GenericRow(
                prevLine.split(ioschema.outputRowFormatMap("TOK_TABLEROWFORMATFIELD"), 2)
                .asInstanceOf[Array[Any]])
            }
          } else {
            val ret = deserialize()
            if (!eof) {
              cacheRow = null
              cacheRow = deserialize()
            }
            ret
          }
        }
      }

      writerThread.start()

      outputIterator
    }

    child.execute().mapPartitions { iter =>
      if (iter.hasNext) {
        processIterator(iter)
      } else {
        // If the input iterator has no rows then do not launch the external script.
        Iterator.empty
      }
    }
  }
}

private class ScriptTransformationWriterThread(
    iter: Iterator[Row],
    outputProjection: Projection,
    @Nullable inputSerde: AbstractSerDe,
    @Nullable inputSoi: ObjectInspector,
    ioschema: HiveScriptIOSchema,
    outputStream: OutputStream,
    proc: Process,
    stderrBuffer: CircularBuffer,
    taskContext: TaskContext)
  extends Thread("Thread-ScriptTransformation-Feed") with Logging {

  setDaemon(true)

  @volatile private var _exception: Throwable = null

  /** Contains the exception thrown while writing the parent iterator to the external process. */
  def exception: Option[Throwable] = Option(_exception)

  override def run(): Unit = Utils.logUncaughtExceptions {
    TaskContext.setTaskContext(taskContext)

    val dataOutputStream = new DataOutputStream(outputStream)

    // We can't use Utils.tryWithSafeFinally here because we also need a `catch` block, so
    // let's use a variable to record whether the `finally` block was hit due to an exception
    var threwException: Boolean = true
    try {
      iter.map(outputProjection).foreach { row =>
        if (inputSerde == null) {
          val data = row.mkString(
            "",
            ioschema.inputRowFormatMap("TOK_TABLEROWFORMATFIELD"),
            ioschema.inputRowFormatMap("TOK_TABLEROWFORMATLINES")).getBytes("utf-8")
          outputStream.write(data)
        } else {
          val writable = inputSerde.serialize(row.asInstanceOf[GenericRow].values, inputSoi)
          prepareWritable(writable).write(dataOutputStream)
        }
      }
      outputStream.close()
      threwException = false
    } catch {
      case NonFatal(e) =>
        // An error occurred while writing input, so kill the child process. According to the
        // Javadoc this call will not throw an exception:
        _exception = e
        proc.destroy()
        throw e
    } finally {
      try {
        if (proc.waitFor() != 0) {
          logError(stderrBuffer.toString) // log the stderr circular buffer
        }
      } catch {
        case NonFatal(exceptionFromFinallyBlock) =>
          if (!threwException) {
            throw exceptionFromFinallyBlock
          } else {
            log.error("Exception in finally block", exceptionFromFinallyBlock)
          }
      }
    }
  }
}

/**
 * The wrapper class of Hive input and output schema properties
 */
private[hive]
case class HiveScriptIOSchema (
    inputRowFormat: Seq[(String, String)],
    outputRowFormat: Seq[(String, String)],
    inputSerdeClass: String,
    outputSerdeClass: String,
    inputSerdeProps: Seq[(String, String)],
    outputSerdeProps: Seq[(String, String)],
    schemaLess: Boolean) extends ScriptInputOutputSchema with HiveInspectors {

  val defaultFormat = Map(("TOK_TABLEROWFORMATFIELD", "\t"),
                          ("TOK_TABLEROWFORMATLINES", "\n"))

  val inputRowFormatMap = inputRowFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputRowFormatMap = outputRowFormat.toMap.withDefault((k) => defaultFormat(k))


  def initInputSerDe(input: Seq[Expression]): (AbstractSerDe, ObjectInspector) = {
    val (columns, columnTypes) = parseAttrs(input)
    val serde = initSerDe(inputSerdeClass, columns, columnTypes, inputSerdeProps)
    (serde, initInputSoi(serde, columns, columnTypes))
  }

  def initOutputSerDe(output: Seq[Attribute]): (AbstractSerDe, StructObjectInspector) = {
    val (columns, columnTypes) = parseAttrs(output)
    val serde = initSerDe(outputSerdeClass, columns, columnTypes, outputSerdeProps)
    (serde, initOutputputSoi(serde))
  }

  def parseAttrs(attrs: Seq[Expression]): (Seq[String], Seq[DataType]) = {

    val columns = attrs.map {
      case aref: AttributeReference => aref.name
      case e: NamedExpression => e.name
      case _ => null
    }

    val columnTypes = attrs.map {
      case aref: AttributeReference => aref.dataType
      case e: NamedExpression => e.dataType
      case _ =>  null
    }

    (columns, columnTypes)
  }

  def initSerDe(serdeClassName: String, columns: Seq[String],
    columnTypes: Seq[DataType], serdeProps: Seq[(String, String)]): AbstractSerDe = {

    val serde: AbstractSerDe = if (serdeClassName != "") {
      val trimed_class = serdeClassName.split("'")(1)
      Utils.classForName(trimed_class)
        .newInstance.asInstanceOf[AbstractSerDe]
    } else {
      null
    }

    if (serde != null) {
      val columnTypesNames = columnTypes.map(_.toTypeInfo.getTypeName()).mkString(",")

      var propsMap = serdeProps.map(kv => {
        (kv._1.split("'")(1), kv._2.split("'")(1))
      }).toMap + (serdeConstants.LIST_COLUMNS -> columns.mkString(","))
      propsMap = propsMap + (serdeConstants.LIST_COLUMN_TYPES -> columnTypesNames)

      val properties = new Properties()
      properties.putAll(propsMap)
      serde.initialize(null, properties)
    }

    serde
  }

  def initInputSoi(inputSerde: AbstractSerDe, columns: Seq[String], columnTypes: Seq[DataType])
    : ObjectInspector = {

    if (inputSerde != null) {
      val fieldObjectInspectors = columnTypes.map(toInspector(_))
      ObjectInspectorFactory
        .getStandardStructObjectInspector(columns, fieldObjectInspectors)
        .asInstanceOf[ObjectInspector]
    } else {
      null
    }
  }

  def initOutputputSoi(outputSerde: AbstractSerDe): StructObjectInspector = {
    if (outputSerde != null) {
      outputSerde.getObjectInspector().asInstanceOf[StructObjectInspector]
    } else {
      null
    }
  }
}

/**
 * An `OutputStream` that will store the last `sizeBytes` data written to it in a circular buffer.
 * The current contents of the buffer can be accessed using the `toString()` method.
 */
private class CircularBuffer(sizeInBytes: Int) extends OutputStream {
  var pos: Int = 0
  var buffer = new Array[Int](sizeInBytes)

  def write(i: Int): Unit = {
    buffer(pos) = i
    pos = (pos + 1) % buffer.length
  }

  override def toString(): String = {
    val (end, start) = buffer.splitAt(pos)
    val input = new InputStream {
      val iterator = (start ++ end).iterator

      def read(): Int = if (iterator.hasNext) iterator.next() else -1
    }
    val reader = new BufferedReader(new InputStreamReader(input))
    val stringBuilder = new StringBuilder
    var line = reader.readLine()
    while (line != null) {
      stringBuilder.append(line)
      stringBuilder.append("\n")
      line = reader.readLine()
    }
    stringBuilder.toString()
  }
}
