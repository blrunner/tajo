/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.FunctionContext;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

/**
 * This is the sort-based aggregation operator.
 *
 * <h3>Implementation</h3>
 * Sort Aggregation has two states while running.
 *
 * <h4>Aggregate state</h4>
 * If lastkey is null or lastkey is equivalent to the current key, sort aggregation is changed to this state.
 * In this state, this operator aggregates measure values via aggregation functions.
 *
 * <h4>Finalize state</h4>
 * If currentKey is different from the last key, it computes final aggregation results, and then
 * it makes an output tuple.
 */
public class SortAggregateExec extends AggregationExec {
  private Tuple lastKey = null;
  private boolean finished = false;
  private FunctionContext contexts[];

  public SortAggregateExec(TaskAttemptContext context, GroupbyNode plan, PhysicalExec child) throws IOException {
    super(context, plan, child);

    contexts = new FunctionContext[plan.getAggFunctions() == null ? 0 : plan.getAggFunctions().length];
  }

  @Override
  public Tuple next() throws IOException {
    Tuple currentKey;
    Tuple tuple = null;
    Tuple outputTuple = null;
    int nullCount = 0;

    while(!context.isStopped() && (tuple = child.next()) != null) {
      // get a key tuple
      currentKey = new VTuple(groupingKeyIds.length);
      for(int i = 0; i < groupingKeyIds.length; i++) {
        currentKey.put(i, tuple.get(groupingKeyIds[i]));
      }

      /** Aggregation State */
      if (lastKey == null || lastKey.equals(currentKey)) {
        if (lastKey == null) {
          for(int i = 0; i < aggFunctionsNum; i++) {
            contexts[i] = aggFunctions[i].newContext();
            aggFunctions[i].merge(contexts[i], inSchema, tuple);

            // Find NullDatum in a tuple
            if (groupingKeyNum == 0 && aggFunctionsNum == tuple.size()
                && tuple.get(i) == NullDatum.get()) {
              nullCount++;
            }
          }
          lastKey = currentKey;
        } else {
          // aggregate
          for (int i = 0; i < aggFunctionsNum; i++) {
            aggFunctions[i].merge(contexts[i], inSchema, tuple);
          }
        }

      } else { /** Finalization State */
        // finalize aggregate and return
        outputTuple = new VTuple(outSchema.size());
        int tupleIdx = 0;

        for(; tupleIdx < groupingKeyNum; tupleIdx++) {
          outputTuple.put(tupleIdx, lastKey.get(tupleIdx));
        }
        for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
          outputTuple.put(tupleIdx, aggFunctions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
        }

        for(int evalIdx = 0; evalIdx < aggFunctionsNum; evalIdx++) {
          contexts[evalIdx] = aggFunctions[evalIdx].newContext();
          aggFunctions[evalIdx].merge(contexts[evalIdx], inSchema, tuple);
        }

        lastKey = currentKey;
        return outputTuple;
      }
    } // while loop

    if (tuple == null && lastKey == null) {
      finished = true;
      return null;
    }
    if (!finished) {
      outputTuple = new VTuple(outSchema.size());
      int tupleIdx = 0;
      for(; tupleIdx < groupingKeyNum; tupleIdx++) {
        outputTuple.put(tupleIdx, lastKey.get(tupleIdx));
      }
      for(int aggFuncIdx = 0; aggFuncIdx < aggFunctionsNum; tupleIdx++, aggFuncIdx++) {
        outputTuple.put(tupleIdx, aggFunctions[aggFuncIdx].terminate(contexts[aggFuncIdx]));
      }
      finished = true;
    }

    // If SortAggregateExec received NullDatum and didn't has any grouping keys,
    // it should return primitive values for NullDatum.
    if (finished && groupingKeyNum == 0 && aggFunctionsNum > 0 && nullCount == aggFunctionsNum) {
      NullDatum nullDatum = DatumFactory.createNullDatum();

      for (int i = 0; i < outColumnNum; i++) {
        TajoDataTypes.Type type = outSchema.getColumn(i).getDataType().getType();
        if (type == TajoDataTypes.Type.INT8) {
          outputTuple.put(i, DatumFactory.createInt8(nullDatum.asInt8()));
        } else if (type == TajoDataTypes.Type.INT4) {
          outputTuple.put(i, DatumFactory.createInt4(nullDatum.asInt4()));
        } else if (type == TajoDataTypes.Type.INT2) {
          outputTuple.put(i, DatumFactory.createInt2(nullDatum.asInt2()));
        } else if (type == TajoDataTypes.Type.FLOAT4) {
          outputTuple.put(i, DatumFactory.createFloat4(nullDatum.asFloat4()));
        } else if (type == TajoDataTypes.Type.FLOAT8) {
          outputTuple.put(i, DatumFactory.createFloat8(nullDatum.asFloat8()));
        } else {
          outputTuple.put(i, DatumFactory.createNullDatum());
        }
      }
    }

    return outputTuple;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastKey = null;
    finished = false;
  }
}
