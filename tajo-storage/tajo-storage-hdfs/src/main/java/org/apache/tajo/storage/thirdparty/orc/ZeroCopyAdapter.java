/*
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

package org.apache.tajo.storage.thirdparty.orc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class ZeroCopyAdapter {
  private final FSDataInputStream in;
  private final ByteBufferPoolAdapter pool;
  private final static EnumSet<ReadOption> CHECK_SUM = EnumSet
      .noneOf(ReadOption.class);
  private final static EnumSet<ReadOption> NO_CHECK_SUM = EnumSet
      .of(ReadOption.SKIP_CHECKSUMS);

  public ZeroCopyAdapter(FSDataInputStream in, ByteBufferAllocatorPool poolshim) {
    this.in = in;
    if (poolshim != null) {
      pool = new ByteBufferPoolAdapter(poolshim);
    } else {
      pool = null;
    }
  }

  public final ByteBuffer readBuffer(int maxLength, boolean verifyChecksums)
      throws IOException {
    EnumSet<ReadOption> options = NO_CHECK_SUM;
    if (verifyChecksums) {
      options = CHECK_SUM;
    }
    return this.in.read(this.pool, maxLength, options);
  }

  public final void releaseBuffer(ByteBuffer buffer) {
    this.in.releaseBuffer(buffer);
  }
}
