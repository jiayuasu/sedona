/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.expressions.raster

import org.apache.sedona.common.raster.RasterOutputs
import org.apache.sedona.common.raster.cog.CogOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.sedona_sql.UDT.RasterUDT
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.InferrableRasterTypes._
import org.apache.spark.sql.sedona_sql.expressions.InferredExpression
import org.apache.spark.sql.sedona_sql.expressions.raster.implicits.RasterInputExpressionEnhancer
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, DoubleType, IntegerType, StringType}

private[apache] case class RS_AsGeoTiff(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterOutputs.asGeoTiff),
      inferrableFunction1(RasterOutputs.asGeoTiff)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsArcGrid(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterOutputs.asArcGrid),
      inferrableFunction1(RasterOutputs.asArcGrid)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsPNG(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterOutputs.asPNG),
      inferrableFunction2(RasterOutputs.asPNG)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsBase64(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(RasterOutputs.asBase64),
      inferrableFunction2(RasterOutputs.asBase64)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsMatrix(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(RasterOutputs.asMatrix),
      inferrableFunction2(RasterOutputs.asMatrix),
      inferrableFunction1(RasterOutputs.asMatrix)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsImage(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(RasterOutputs.createHTMLString),
      inferrableFunction1(RasterOutputs.createHTMLString)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class RS_AsCOG(inputExpressions: Seq[Expression])
    extends Expression
    with CodegenFallback
    with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def dataType: DataType = BinaryType

  override def eval(input: InternalRow): Any = {
    val raster = inputExpressions(0).toRaster(input)
    if (raster == null) return null

    val builder = CogOptions.builder()
    if (inputExpressions.length >= 2) {
      builder.compression(
        inputExpressions(1)
          .eval(input)
          .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
          .toString)
    }
    if (inputExpressions.length >= 3) {
      builder.tileSize(inputExpressions(2).eval(input).asInstanceOf[Int])
    }
    if (inputExpressions.length >= 4) {
      builder.compressionQuality(inputExpressions(3).eval(input).asInstanceOf[Double])
    }
    if (inputExpressions.length >= 5) {
      builder.resampling(
        inputExpressions(4)
          .eval(input)
          .asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
          .toString)
    }
    if (inputExpressions.length >= 6) {
      builder.overviewCount(inputExpressions(5).eval(input).asInstanceOf[Int])
    }

    RasterOutputs.asCloudOptimizedGeoTiff(raster, builder.build())
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): RS_AsCOG = {
    copy(inputExpressions = newChildren)
  }

  override def inputTypes: Seq[AbstractDataType] = {
    val base = Seq[AbstractDataType](RasterUDT())
    val optional = Seq(StringType, IntegerType, DoubleType, StringType, IntegerType)
    base ++ optional.take(inputExpressions.length - 1)
  }
}
