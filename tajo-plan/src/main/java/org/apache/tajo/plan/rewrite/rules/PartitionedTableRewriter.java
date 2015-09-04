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

package org.apache.tajo.plan.rewrite.rules;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionsByAlgebraProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionsByDirectSqlProto;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.util.PartitionFilterEvalNodeVisitor;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.plan.util.ScanQualConverter;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.StringUtils;

import java.io.IOException;
import java.util.*;

public class PartitionedTableRewriter implements LogicalPlanRewriteRule {
  private CatalogService catalog;

  private static final Log LOG = LogFactory.getLog(PartitionedTableRewriter.class);

  private static final String NAME = "Partitioned Table Rewriter";
  private final Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    for (LogicalPlan.QueryBlock block : context.getPlan().getQueryBlocks()) {
      for (RelationNode relation : block.getRelations()) {
        if (relation.getType() == NodeType.SCAN) {
          TableDesc table = ((ScanNode)relation).getTableDesc();
          if (table.hasPartition()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlan plan = context.getPlan();
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    this.catalog = context.getCatalog();
    rewriter.visit(context.getQueryContext(), plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private static class PartitionPathFilter implements PathFilter {

    private Schema schema;
    private EvalNode partitionFilter;
    public PartitionPathFilter(Schema schema, EvalNode partitionFilter) {
      this.schema = schema;
      this.partitionFilter = partitionFilter;
      partitionFilter.bind(null, schema);
    }

    @Override
    public boolean accept(Path path) {
      Tuple tuple = buildTupleFromPartitionPath(schema, path, true);
      if (tuple == null) { // if it is a file or not acceptable file
        return false;
      }

      return partitionFilter.eval(tuple).asBool();
    }

    @Override
    public String toString() {
      return partitionFilter.toString();
    }
  }

  /**
   * It assumes that each conjunctive form corresponds to one column.
   *
   * @param partitionColumns
   * @param conjunctiveForms search condition corresponding to partition columns.
   *                         If it is NULL, it means that there is no search condition for this table.
   * @param tablePath
   * @return
   * @throws IOException
   */
  private Path [] findFilteredPaths(OverridableConf queryContext, String tableName,
                                    Schema partitionColumns, EvalNode [] conjunctiveForms, Path tablePath)
      throws IOException, UndefinedDatabaseException, UndefinedTableException,
      UndefinedPartitionMethodException, UndefinedOperatorException {

    Path [] filteredPaths = null;
    FileSystem fs = tablePath.getFileSystem(queryContext.getConf());
    String [] splits = CatalogUtil.splitFQTableName(tableName);
    List<PartitionDescProto> partitions = null;

    String store = queryContext.getConf().get(CatalogConstants.STORE_CLASS);

    try {
      // HiveCatalogStore provides list of table partitions with where clause because hive just provides api using
      // the filter string. So, this rewriter need to differentiate HiveCatalogStore and other catalogs.
      if (store.equals("org.apache.tajo.catalog.store.HiveCatalogStore")) {
        PartitionsByDirectSqlProto request = buildDirectSQLWithHiveCatalogStore(splits[0], splits[1],
          partitionColumns, conjunctiveForms);

        if ((conjunctiveForms == null) || (conjunctiveForms != null && !request.getDirectSql().equals(""))) {
          partitions = catalog.getPartitionsByDirectSql(request);
        }
      } else {
        // Only when exists table partitions on catalog, try to get list of table partitions.
        if (catalog.existPartitions(splits[0], splits[1])) {
          // If there are no filters, this don't need to build direct sql for getting table partitions.
          if (conjunctiveForms == null) {
            partitions = catalog.getAllPartitions(splits[0], splits[1]);
          } else {
            PartitionsByAlgebraProto request = buildDirectSQLWithDBStore(splits[0], splits[1], conjunctiveForms);
            partitions = catalog.getPartitionsByAlgebra(request);
          }
        }
      }

      // If catalog returns list of table partitions successfully, build path lists for scanning table data.
      if (partitions != null) {
        filteredPaths = new Path[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
          filteredPaths[i] = new Path(partitions.get(i).getPath());
        }
      }
    } catch (Exception e) {
      partitions = null;
    }

    // If we should fail to build path lists with catalog, we need to path lists using getting an array of FileStatus
    // objects with the path-filter.
    if (partitions == null || filteredPaths == null) {
      PathFilter [] filters;
      if (conjunctiveForms == null) {
        filters = buildAllAcceptingPathFilters(partitionColumns);
      } else {
        filters = buildPathFiltersForAllLevels(partitionColumns, conjunctiveForms);
      }

      // loop from one to the number of partition columns
      filteredPaths = toPathArray(fs.listStatus(tablePath, filters[0]));

      for (int i = 1; i < partitionColumns.size(); i++) {
        // Get all file status matched to a ith level path filter.
        filteredPaths = toPathArray(fs.listStatus(filteredPaths, filters[i]));
      }
    }

    LOG.info("Filtered directory or files: " + filteredPaths.length);

    return filteredPaths;
  }

  /**
   * This will build algebra expressions for querying partitions and partition keys in CatalogStore.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Assume that an user gives a condition WHERE (col1 ='1' or col1 = '100') and col3 > 20 .
   *
   * Then, the algebra expression would be generated as following:
   *
   *  {
   *  "LeftExpr": {
   *    "LeftExpr": {
   *      "Qualifier": "default.table1",
   *      "ColumnName": "col3",
   *      "OpType": "Column"
   *    },
   *    "RightExpr": {
   *      "Value": "20.0",
   *      "ValueType": "Unsigned_Integer",
   *      "OpType": "Literal"
   *    },
   *    "OpType": "GreaterThan"
   *  },
   *  "RightExpr": {
   *    "LeftExpr": {
   *      "LeftExpr": {
   *        "Qualifier": "default.table1",
   *        "ColumnName": "col1",
   *        "OpType": "Column"
   *      },
   *      "RightExpr": {
   *        "Value": "1",
   *        "ValueType": "String",
   *        "OpType": "Literal"
   *      },
   *      "OpType": "Equals"
   *    },
   *    "RightExpr": {
   *      "LeftExpr": {
   *        "Qualifier": "default.table1",
   *        "ColumnName": "col1",
   *        "OpType": "Column"
   *      },
   *      "RightExpr": {
   *        "Value": "100",
   *        "ValueType": "String",
   *        "OpType": "Literal"
   *      },
   *      "OpType": "Equals"
   *    },
   *    "OpType": "Or"
   *  },
   *  "OpType": "And"
   *}
   *
   * @param databaseName
   * @param tableName
   * @param conjunctiveForms
   * @return
   */
  public static PartitionsByAlgebraProto buildDirectSQLWithDBStore(
    String databaseName, String tableName, EvalNode [] conjunctiveForms) {

    PartitionsByAlgebraProto.Builder request = PartitionsByAlgebraProto.newBuilder();
    request.setDatabaseName(databaseName);
    request.setTableName(tableName);

    if (conjunctiveForms != null) {
      EvalNode evalNode = AlgebraicUtil.createSingletonExprFromCNF(conjunctiveForms);
      ScanQualConverter convertor = new ScanQualConverter(databaseName + "." + tableName);
      convertor.visit(null, evalNode, new Stack<EvalNode>());
      request.setAlgebra(convertor.getResult().toJson());
    } else {
      request.setAlgebra("");
    }

    return request.build();
  }

  /**
   * This will build direct sql and parameters for querying partitions and partition keys in HiveCatalogStore.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Assume that an user gives a condition WHERE (col1 ='1' or col1 = '100') and col3 > 20.
   * There is no filter condition corresponding to col2.
   *
   * Then, the sql would be generated as following:
   *
   *  (col1 = \"1\" or col1 = \"100\") and col3 > 20
   *
   * @param databaseName
   * @param tableName
   * @param partitionColumns
   * @param conjunctiveForms
   * @return
   */
  public static PartitionsByDirectSqlProto buildDirectSQLWithHiveCatalogStore(
    String databaseName, String tableName, Schema partitionColumns, EvalNode [] conjunctiveForms) {

    PartitionsByDirectSqlProto.Builder request = PartitionsByDirectSqlProto.newBuilder();
    request.setDatabaseName(databaseName);
    request.setTableName(tableName);

    PartitionFilterEvalNodeVisitor visitor = new PartitionFilterEvalNodeVisitor();
    visitor.setIsHiveCatalog(true);

    EvalNode[] filters = buildEvalNodesForAllLevels(partitionColumns, conjunctiveForms);

    StringBuffer sb = new StringBuffer();

    // Write join clause from second column to last column.
    for (int i = 0; i < partitionColumns.size(); i++) {
      Column target = partitionColumns.getColumn(i);

      // Hive api doesn't support not null statement.
      if (!(filters[i] instanceof IsNullEval)) {
        if (sb.length() > 0) {
          sb.append(" AND ");
        }

        visitor.setColumn(target);
        visitor.visit(null, filters[i], new Stack<EvalNode>());

        // Hive api doesn't support row constant statement;
        if (visitor.isExistRowCostant()) {
          return null;
        }

        sb.append(visitor.getResult());
        visitor.clearParameters();
      }
    }
    request.setDirectSql(sb.toString());

    return request.build();
  }

  /**
   * Build EvalNodes for all columns with a list of filter conditions.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Then, this methods will create three EvalNodes for (col1), (col2), (col3).
   *
   * Assume that an user gives a condition WHERE col1 ='A' and col3 = 'C'.
   * There is no filter condition corresponding to col2.
   * Then, the path filter conditions are corresponding to the followings:
   *
   * The first EvalNode: col1 = 'A'
   * The second EvalNode: col2 IS NOT NULL
   * The third EvalNode: col3 = 'C'
   *
   * 'IS NOT NULL' predicate is always true against the partition path.
   *
   * @param partitionColumns
   * @param conjunctiveForms
   * @return
   */
  private static EvalNode[] buildEvalNodesForAllLevels(Schema partitionColumns, EvalNode[] conjunctiveForms) {
    EvalNode[] filters = new EvalNode[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    Column target;

    for (int i = 0; i < partitionColumns.size(); i++) {
      target = partitionColumns.getColumn(i);

      if (conjunctiveForms == null) {
        accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));
      } else {
        for (EvalNode expr : conjunctiveForms) {
          if (EvalTreeUtil.findUniqueColumns(expr).contains(target)) {
            // Accumulate one qual per level
            accumulatedFilters.add(expr);
          }
        }

        if (accumulatedFilters.size() < (i + 1)) {
          accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));
        }
      }
      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
        accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = filterPerLevel;

    }

    return filters;
  }

  /**
   * Build path filters for all levels with a list of filter conditions.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Then, this methods will create three path filters for (col1), (col1, col2), (col1, col2, col3).
   *
   * Corresponding filter conditions will be placed on each path filter,
   * If there is no corresponding expression for certain column,
   * The condition will be filled with a true value.
   *
   * Assume that an user gives a condition WHERE col1 ='A' and col3 = 'C'.
   * There is no filter condition corresponding to col2.
   * Then, the path filter conditions are corresponding to the followings:
   *
   * The first path filter: col1 = 'A'
   * The second path filter: col1 = 'A' AND col2 IS NOT NULL
   * The third path filter: col1 = 'A' AND col2 IS NOT NULL AND col3 = 'C'
   *
   * 'IS NOT NULL' predicate is always true against the partition path.
   *
   * @param partitionColumns
   * @param conjunctiveForms
   * @return
   */
  private static PathFilter [] buildPathFiltersForAllLevels(Schema partitionColumns,
                                                     EvalNode [] conjunctiveForms) {
    // Building partition path filters for all levels
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);

      for (EvalNode expr : conjunctiveForms) {
        if (EvalTreeUtil.findUniqueColumns(expr).contains(target)) {
          // Accumulate one qual per level
          accumulatedFilters.add(expr);
        }
      }

      if (accumulatedFilters.size() < (i + 1)) {
        accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));
      }

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
          accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }

    return filters;
  }

  /**
   * Build an array of path filters for all levels with all accepting filter condition.
   * @param partitionColumns The partition columns schema
   * @return The array of path filter, accpeting all partition paths.
   */
  private static PathFilter [] buildAllAcceptingPathFilters(Schema partitionColumns) {
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);
      accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
          accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }
    return filters;
  }

  private static Path [] toPathArray(FileStatus[] fileStatuses) {
    Path [] paths = new Path[fileStatuses.length];
    for (int j = 0; j < fileStatuses.length; j++) {
      paths[j] = fileStatuses[j].getPath();
    }
    return paths;
  }

  public Path [] findFilteredPartitionPaths(OverridableConf queryContext, ScanNode scanNode) throws IOException,
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
    UndefinedOperatorException {
    TableDesc table = scanNode.getTableDesc();
    PartitionMethodDesc partitionDesc = scanNode.getTableDesc().getPartitionMethod();

    Schema paritionValuesSchema = new Schema();
    for (Column column : partitionDesc.getExpressionSchema().getRootColumns()) {
      paritionValuesSchema.addColumn(column);
    }

    Set<EvalNode> indexablePredicateSet = Sets.newHashSet();

    // if a query statement has a search condition, try to find indexable predicates
    if (scanNode.hasQual()) {
      EvalNode [] conjunctiveForms = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());
      Set<EvalNode> remainExprs = Sets.newHashSet(conjunctiveForms);

      // add qualifier to schema for qual
      paritionValuesSchema.setQualifier(scanNode.getCanonicalName());
      for (Column column : paritionValuesSchema.getRootColumns()) {
        for (EvalNode simpleExpr : conjunctiveForms) {
          if (checkIfIndexablePredicateOnTargetColumn(simpleExpr, column)) {
            indexablePredicateSet.add(simpleExpr);
          }
        }
      }

      // Partitions which are not matched to the partition filter conditions are pruned immediately.
      // So, the partition filter conditions are not necessary later, and they are removed from
      // original search condition for simplicity and efficiency.
      remainExprs.removeAll(indexablePredicateSet);
      if (remainExprs.isEmpty()) {
        scanNode.setQual(null);
      } else {
        scanNode.setQual(
            AlgebraicUtil.createSingletonExprFromCNF(remainExprs.toArray(new EvalNode[remainExprs.size()])));
      }
    }

    if (indexablePredicateSet.size() > 0) { // There are at least one indexable predicates
      return findFilteredPaths(queryContext, table.getName(), paritionValuesSchema,
          indexablePredicateSet.toArray(new EvalNode[indexablePredicateSet.size()]), new Path(table.getUri()));
    } else { // otherwise, we will get all partition paths.
      return findFilteredPaths(queryContext, table.getName(), paritionValuesSchema, null, new Path(table.getUri()));
    }
  }

  private boolean checkIfIndexablePredicateOnTargetColumn(EvalNode evalNode, Column targetColumn) {
    if (checkIfIndexablePredicate(evalNode) || checkIfDisjunctiveButOneVariable(evalNode)) {
      Set<Column> variables = EvalTreeUtil.findUniqueColumns(evalNode);
      // if it contains only single variable matched to a target column
      return variables.size() == 1 && variables.contains(targetColumn);
    } else {
      return false;
    }
  }

  /**
   * Check if an expression consists of one variable and one constant and
   * the expression is a comparison operator.
   *
   * @param evalNode The expression to be checked
   * @return true if an expression consists of one variable and one constant
   * and the expression is a comparison operator. Other, false.
   */
  private boolean checkIfIndexablePredicate(EvalNode evalNode) {
    // TODO - LIKE with a trailing wild-card character and IN with an array can be indexable
    return AlgebraicUtil.containSingleVar(evalNode) && AlgebraicUtil.isIndexableOperator(evalNode);
  }

  /**
   *
   * @param evalNode The expression to be checked
   * @return true if an disjunctive expression, consisting of indexable expressions
   */
  private boolean checkIfDisjunctiveButOneVariable(EvalNode evalNode) {
    if (evalNode.getType() == EvalType.OR) {
      BinaryEval orEval = (BinaryEval) evalNode;
      boolean indexable =
          checkIfIndexablePredicate(orEval.getLeftExpr()) &&
              checkIfIndexablePredicate(orEval.getRightExpr());

      boolean sameVariable =
          EvalTreeUtil.findUniqueColumns(orEval.getLeftExpr())
              .equals(EvalTreeUtil.findUniqueColumns(orEval.getRightExpr()));

      return indexable && sameVariable;
    } else {
      return false;
    }
  }

  private void updateTableStat(OverridableConf queryContext, PartitionedTableScanNode scanNode)
      throws TajoException {
    if (scanNode.getInputPaths().length > 0) {
      try {
        FileSystem fs = scanNode.getInputPaths()[0].getFileSystem(queryContext.getConf());
        long totalVolume = 0;

        for (Path input : scanNode.getInputPaths()) {
          ContentSummary summary = fs.getContentSummary(input);
          totalVolume += summary.getLength();
          totalVolume += summary.getFileCount();
        }
        scanNode.getTableDesc().getStats().setNumBytes(totalVolume);
      } catch (Throwable e) {
        throw new TajoInternalError(e);
      }
    }
  }

  /**
   * Take a look at a column partition path. A partition path consists
   * of a table path part and column values part. This method transforms
   * a partition path into a tuple with a given partition column schema.
   *
   * hdfs://192.168.0.1/tajo/warehouse/table1/col1=abc/col2=def/col3=ghi
   *                   ^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^
   *                      table path part        column values part
   *
   * When a file path is given, it can perform two ways depending on beNullIfFile flag.
   * If it is true, it returns NULL when a given path is a file.
   * Otherwise, it returns a built tuple regardless of file or directory.
   *
   * @param partitionColumnSchema The partition column schema
   * @param partitionPath The partition path
   * @param beNullIfFile If true, this method returns NULL when a given path is a file.
   * @return The tuple transformed from a column values part.
   */
  public static Tuple buildTupleFromPartitionPath(Schema partitionColumnSchema, Path partitionPath,
                                                  boolean beNullIfFile) {
    int startIdx = partitionPath.toString().indexOf(getColumnPartitionPathPrefix(partitionColumnSchema));

    if (startIdx == -1) { // if there is no partition column in the patch
      return null;
    }
    String columnValuesPart = partitionPath.toString().substring(startIdx);

    String [] columnValues = columnValuesPart.split("/");

    // true means this is a file.
    if (beNullIfFile && partitionColumnSchema.size() < columnValues.length) {
      return null;
    }

    Tuple tuple = new VTuple(partitionColumnSchema.size());
    int i = 0;
    for (; i < columnValues.length && i < partitionColumnSchema.size(); i++) {
      String [] parts = columnValues[i].split("=");
      if (parts.length != 2) {
        return null;
      }
      int columnId = partitionColumnSchema.getColumnIdByName(parts[0]);
      Column keyColumn = partitionColumnSchema.getColumn(columnId);
      tuple.put(columnId, DatumFactory.createFromString(keyColumn.getDataType(), StringUtils.unescapePathName(parts[1])));
    }
    for (; i < partitionColumnSchema.size(); i++) {
      tuple.put(i, NullDatum.get());
    }
    return tuple;
  }

  /**
   * Get a prefix of column partition path. For example, consider a column partition (col1, col2).
   * Then, you will get a string 'col1='.
   *
   * @param partitionColumn the schema of column partition
   * @return The first part string of column partition path.
   */
  private static String getColumnPartitionPathPrefix(Schema partitionColumn) {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionColumn.getColumn(0).getSimpleName()).append("=");
    return sb.toString();
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<OverridableConf, Object> {
    @Override
    public Object visitScan(OverridableConf queryContext, LogicalPlan plan, LogicalPlan.QueryBlock block,
                            ScanNode scanNode, Stack<LogicalNode> stack) throws TajoException {

      TableDesc table = scanNode.getTableDesc();
      if (!table.hasPartition()) {
        return null;
      }

      try {
        Path [] filteredPaths = findFilteredPartitionPaths(queryContext, scanNode);
        plan.addHistory("PartitionTableRewriter chooses " + filteredPaths.length + " of partitions");
        PartitionedTableScanNode rewrittenScanNode = plan.createNode(PartitionedTableScanNode.class);
        rewrittenScanNode.init(scanNode, filteredPaths);
        updateTableStat(queryContext, rewrittenScanNode);

        // if it is topmost node, set it as the rootnode of this block.
        if (stack.empty() || block.getRoot().equals(scanNode)) {
          block.setRoot(rewrittenScanNode);
        } else {
          PlannerUtil.replaceNode(plan, stack.peek(), scanNode, rewrittenScanNode);
        }
        block.registerNode(rewrittenScanNode);
      } catch (IOException e) {
        throw new TajoInternalError("Partitioned Table Rewrite Failed: \n" + e.getMessage());
      }
      return null;
    }
  }
}
