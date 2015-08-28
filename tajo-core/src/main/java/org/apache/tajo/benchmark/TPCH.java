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

package org.apache.tajo.benchmark;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.storage.StorageConstants;

import java.io.IOException;
import java.util.Map;

public class TPCH extends BenchmarkSet {
  private final String BENCHMARK_DIR = "benchmark/tpch";

  public static final String LINEITEM = "lineitem";
  public static final String CUSTOMER = "customer";
  public static final String CUSTOMER_PARTS = "customer_parts";
  public static final String NATION = "nation";
  public static final String PART = "part";
  public static final String REGION = "region";
  public static final String ORDERS = "orders";
  public static final String PARTSUPP = "partsupp";
  public static final String SUPPLIER = "supplier";
  public static final String EMPTY_ORDERS = "empty_orders";


  public static final Map<String, Long> tableVolumes = Maps.newHashMap();
  public static String PARTITION_TABLE;

  static {
    tableVolumes.put(LINEITEM, 759863287L);
    tableVolumes.put(CUSTOMER, 24346144L);
    tableVolumes.put(CUSTOMER_PARTS, 707L);
    tableVolumes.put(NATION, 2224L);
    tableVolumes.put(PART, 24135125L);
    tableVolumes.put(REGION, 389L);
    tableVolumes.put(ORDERS, 171952161L);
    tableVolumes.put(PARTSUPP, 118984616L);
    tableVolumes.put(SUPPLIER, 1409184L);
    tableVolumes.put(EMPTY_ORDERS, 0L);
    PARTITION_TABLE = System.getProperty("partition-table");
  }

  @Override
  public void loadSchemas() {
    Schema lineitem = new Schema()
        .addColumn("l_orderkey", Type.INT4) // 0
        .addColumn("l_partkey", Type.INT4) // 1
        .addColumn("l_suppkey", Type.INT4) // 2
        .addColumn("l_linenumber", Type.INT4) // 3
        .addColumn("l_quantity", Type.FLOAT8) // 4
        .addColumn("l_extendedprice", Type.FLOAT8) // 5
        .addColumn("l_discount", Type.FLOAT8) // 6
        .addColumn("l_tax", Type.FLOAT8) // 7
        // TODO - This is temporal solution. 8 and 9 are actually Char type.
        .addColumn("l_returnflag", Type.TEXT); // 8

    lineitem.addColumn("l_linestatus", Type.TEXT); // 9

    // TODO - This is temporal solution. 10,11, and 12 are actually Date type.
    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      lineitem.addColumn("l_shipdate", Type.TEXT); // 10
    }

    lineitem.addColumn("l_commitdate", Type.TEXT) // 11
        .addColumn("l_receiptdate", Type.TEXT) // 12
        .addColumn("l_shipinstruct", Type.TEXT) // 13
        .addColumn("l_shipmode", Type.TEXT) // 14
        .addColumn("l_comment", Type.TEXT); // 15
    schemas.put(LINEITEM, lineitem);

    Schema customer = new Schema()
        .addColumn("c_custkey", Type.INT4) // 0
        .addColumn("c_name", Type.TEXT) // 1
        .addColumn("c_address", Type.TEXT); // 2

    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      customer.addColumn("c_nationkey", Type.INT4); // 3
    }

    customer.addColumn("c_phone", Type.TEXT) // 4
        .addColumn("c_acctbal", Type.FLOAT8) // 5
        .addColumn("c_mktsegment", Type.TEXT) // 6
        .addColumn("c_comment", Type.TEXT); // 7
    schemas.put(CUSTOMER, customer);

    Schema customerParts = new Schema()
        .addColumn("c_custkey", Type.INT4) // 0
        .addColumn("c_name", Type.TEXT) // 1
        .addColumn("c_address", Type.TEXT) // 2
        .addColumn("c_phone", Type.TEXT) // 3
        .addColumn("c_acctbal", Type.FLOAT8) // 4
        .addColumn("c_mktsegment", Type.TEXT) // 5
        .addColumn("c_comment", Type.TEXT); // 6
    schemas.put(CUSTOMER_PARTS, customerParts);

    Schema nation = new Schema()
        .addColumn("n_nationkey", Type.INT4) // 0
        .addColumn("n_name", Type.TEXT); // 1

    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      nation.addColumn("n_regionkey", Type.INT4); // 2
    }

    nation.addColumn("n_comment", Type.TEXT); // 3
    schemas.put(NATION, nation);

    Schema part = new Schema()
        .addColumn("p_partkey", Type.INT4) // 0
        .addColumn("p_name", Type.TEXT) // 1
        .addColumn("p_mfgr", Type.TEXT) // 2
        .addColumn("p_brand", Type.TEXT) // 3
        .addColumn("p_type", Type.TEXT); // 4

    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      part.addColumn("p_size", Type.INT4); // 5
    }

    part.addColumn("p_container", Type.TEXT) // 6
        .addColumn("p_retailprice", Type.FLOAT8) // 7
        .addColumn("p_comment", Type.TEXT); // 8
    schemas.put(PART, part);

    Schema region = new Schema();

    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      region.addColumn("r_regionkey", Type.INT4); // 0
    }

    region.addColumn("r_name", Type.TEXT) // 1
        .addColumn("r_comment", Type.TEXT); // 2
    schemas.put(REGION, region);

    Schema orders = new Schema()
        .addColumn("o_orderkey", Type.INT4) // 0
        .addColumn("o_custkey", Type.INT4) // 1
        .addColumn("o_orderstatus", Type.TEXT) // 2
        .addColumn("o_totalprice", Type.FLOAT8); // 3

    // TODO - This is temporal solution. o_orderdate is actually Date type.
    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      orders.addColumn("o_orderdate", Type.TEXT); // 4
    }

    orders.addColumn("o_orderpriority", Type.TEXT) // 5
        .addColumn("o_clerk", Type.TEXT) // 6
        .addColumn("o_shippriority", Type.INT4) // 7
        .addColumn("o_comment", Type.TEXT); // 8
    schemas.put(ORDERS, orders);
    schemas.put(EMPTY_ORDERS, orders);


    Schema partsupp = new Schema()
        .addColumn("ps_partkey", Type.INT4) // 0
        .addColumn("ps_suppkey", Type.INT4) // 1
        .addColumn("ps_availqty", Type.INT4) // 2
        .addColumn("ps_supplycost", Type.FLOAT8) // 3
        .addColumn("ps_comment", Type.TEXT); // 4
    schemas.put(PARTSUPP, partsupp);

    Schema supplier = new Schema()
        .addColumn("s_suppkey", Type.INT4) // 0
        .addColumn("s_name", Type.TEXT) // 1
        .addColumn("s_address", Type.TEXT); // 2

    if (PARTITION_TABLE == null || PARTITION_TABLE.equals("")) {
      supplier.addColumn("s_nationkey", Type.INT4); // 3
    }

    supplier.addColumn("s_phone", Type.TEXT) // 4
      .addColumn("s_acctbal", Type.FLOAT8) // 5
        .addColumn("s_comment", Type.TEXT); // 6
    schemas.put(SUPPLIER, supplier);
  }

  public void loadOutSchema() {
    Schema q2 = new Schema()
        .addColumn("s_acctbal", Type.FLOAT8)
        .addColumn("s_name", Type.TEXT)
        .addColumn("n_name", Type.TEXT)
        .addColumn("p_partkey", Type.INT4)
        .addColumn("p_mfgr", Type.TEXT)
        .addColumn("s_address", Type.TEXT)
        .addColumn("s_phone", Type.TEXT)
        .addColumn("s_comment", Type.TEXT);
    outSchemas.put("q2", q2);
  }

  public void loadQueries() throws IOException {
    loadQueries(BENCHMARK_DIR);
  }

  public void loadTables() throws TajoException {
    loadTable(LINEITEM);
    loadTable(CUSTOMER);
    loadTable(CUSTOMER_PARTS);
    loadTable(NATION);
    loadTable(PART);
    loadTable(REGION);
    loadTable(ORDERS);
    loadTable(PARTSUPP) ;
    loadTable(SUPPLIER);
    loadTable(EMPTY_ORDERS);

  }

  public PartitionMethodDesc getPartitionMethodDesc(String tableName) {
    PartitionMethodDesc partitionMethodDesc = null;

    if (tableName.equals(LINEITEM)) {
      Schema expressionSchema = new Schema().addColumn("l_shipdate", Type.TEXT);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME,  LINEITEM,
        CatalogProtos.PartitionType.COLUMN, "l_shipdate", expressionSchema);
    } else if (tableName.equals(CUSTOMER)) {
      Schema expressionSchema = new Schema().addColumn("c_nationkey", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, CUSTOMER,
        CatalogProtos.PartitionType.COLUMN, "c_nationkey", expressionSchema);
    } else if (tableName.equals(NATION)) {
      Schema expressionSchema = new Schema().addColumn("n_regionkey", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, NATION,
        CatalogProtos.PartitionType.COLUMN, "n_regionkey", expressionSchema);
    } else if (tableName.equals(PART)) {
      Schema expressionSchema = new Schema().addColumn("p_size", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, PART,
        CatalogProtos.PartitionType.COLUMN, "p_size", expressionSchema);
    } else if (tableName.equals(REGION)) {
      Schema expressionSchema = new Schema().addColumn("r_regionkey", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, REGION,
        CatalogProtos.PartitionType.COLUMN, "r_regionkey", expressionSchema);
    } else if (tableName.equals(ORDERS)) {
      Schema expressionSchema = new Schema().addColumn("o_orderdate", Type.TEXT);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, ORDERS,
        CatalogProtos.PartitionType.COLUMN, "o_orderdate", expressionSchema);
    } else if (tableName.equals(SUPPLIER)) {
      Schema expressionSchema = new Schema().addColumn("s_nationkey", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(TajoConstants.DEFAULT_DATABASE_NAME, SUPPLIER,
        CatalogProtos.PartitionType.COLUMN, "s_nationkey", expressionSchema);
    }

    return partitionMethodDesc;
  }

  public void loadTable(String tableName) throws TajoException {
    TableMeta meta = CatalogUtil.newTableMeta("TEXT");
    meta.putOption(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);

    PartitionMethodDesc partitionMethodDesc = null;
    if (tableName.equals(CUSTOMER_PARTS)) {
      Schema expressionSchema = new Schema();
      expressionSchema.addColumn("c_nationkey", TajoDataTypes.Type.INT4);
      partitionMethodDesc = new PartitionMethodDesc(
          tajo.getCurrentDatabase(),
          CUSTOMER_PARTS,
          CatalogProtos.PartitionType.COLUMN,
          "c_nationkey",
          expressionSchema);
    }

    tajo.createExternalTable(tableName, getSchema(tableName),
        new Path(dataDir, tableName).toUri(), meta, partitionMethodDesc);
  }
}
