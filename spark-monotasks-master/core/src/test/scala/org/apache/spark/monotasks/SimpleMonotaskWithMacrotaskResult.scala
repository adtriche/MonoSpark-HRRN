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

package org.apache.spark.monotasks

import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, TaskContextImpl}
import org.apache.spark.storage.BlockManager

/** A minimal subclass of Monotask for use in testing that generates a dummy macrotask result. */
class SimpleMonotaskWithMacrotaskResult(
    val blockManager: BlockManager, context: TaskContextImpl) extends SimpleMonotask(context) {
  override def executeAndHandleExceptions(): Unit = {
    val macrotaskResult = ByteBuffer.allocate(2)
    SparkEnv.get.localDagScheduler.post(TaskSuccess(this, Some(macrotaskResult)))
  }
}
