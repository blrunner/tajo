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

package org.apache.tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlanString;

public class HavingNode extends UnaryNode implements Cloneable {
	@Expose private EvalNode qual;

  public HavingNode(int pid) {
    super(pid, NodeType.HAVING);
  }

	public EvalNode getQual() {
		return this.qual;
	}

	public void setQual(EvalNode qual) {
		this.qual = qual;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HavingNode) {
      HavingNode other = (HavingNode) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    HavingNode selNode = (HavingNode) super.clone();
    selNode.qual = (EvalNode) this.qual.clone();
    
    return selNode;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this).appendTitle(" (").appendTitle(qual.toString()).appendTitle(")");
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Having\": {\"qual\": \"").append(qual.toString()).append("\",");
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema()).append("}");

    return sb.toString()+"\n"
        + getChild().toString();
  }
}
