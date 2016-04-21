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

package org.apache.tajo.catalog;

import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestTableMeta {
  TableMeta meta = null;
  
  @Before
  public void setUp() {
    meta = CatalogUtil.newTableMeta("TEXT");
  }
  
  @Test
  public void testTableMetaTableProto() {
    TableMeta meta1 = CatalogUtil.newTableMeta("TEXT");
    
    TableMeta meta2 = new TableMeta(meta1.getProto());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public final void testClone() throws CloneNotSupportedException {
    TableMeta meta1 = CatalogUtil.newTableMeta("TEXT");
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    assertEquals(meta1.getDataFormat(), meta2.getDataFormat());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testSchema() throws CloneNotSupportedException {
    TableMeta meta1 = CatalogUtil.newTableMeta("TEXT");
    TableMeta meta2 = (TableMeta) meta1.clone();
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testGetStorageType() {
    assertEquals("TEXT", meta.getDataFormat());
  }
  
  @Test
  public void testEqualsObject() {
    TableMeta meta2 = CatalogUtil.newTableMeta("TEXT");
    assertTrue(meta.equals(meta2));
    assertNotSame(meta, meta2);
  }

	@Test
	public void testEqualsObject2() {
		//This testcases should insert more 2 items into one slot.
		//HashMap's default slot count is 16
		//so max_count is 17

		int MAX_COUNT = 17;

		TableMeta meta1 = CatalogUtil.newTableMeta(BuiltinStorages.TEXT);
		for (int i = 0; i < MAX_COUNT; i++) {
			meta1.putProperty("key"+i, "value"+i);
		}

		PrimitiveProtos.KeyValueSetProto.Builder optionBuilder = PrimitiveProtos.KeyValueSetProto.newBuilder();
		for (int i = 1; i <= MAX_COUNT; i++) {
			PrimitiveProtos.KeyValueProto.Builder keyValueBuilder = PrimitiveProtos.KeyValueProto.newBuilder();
			keyValueBuilder.setKey("key"+(MAX_COUNT-i)).setValue("value"+(MAX_COUNT-i));
			optionBuilder.addKeyval(keyValueBuilder);
		}
		TableProto.Builder builder = TableProto.newBuilder();
		builder.setDataFormat(BuiltinStorages.TEXT);
		builder.setParams(optionBuilder);
		TableMeta meta2 = new TableMeta(builder.build());
		assertTrue(meta1.equals(meta2));
	}
  
  @Test
  public void testGetProto() {
    TableProto proto = meta.getProto();
    TableMeta newMeta = new TableMeta(proto);
    assertEquals(meta, newMeta);
  }

  @Test
  public void testToJson() {
    String json = meta.toJson();
    TableMeta fromJson = CatalogGsonHelper.fromJson(json, TableMeta.class);
    assertEquals(meta, fromJson);
    assertEquals(meta.getProto(), fromJson.getProto());
  }
}
