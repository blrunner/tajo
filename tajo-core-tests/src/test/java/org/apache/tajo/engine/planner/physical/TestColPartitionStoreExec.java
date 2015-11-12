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

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.FunctionLoader;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.parser.sql.SQLAnalyzer;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.serder.PlanProto;
import org.apache.tajo.session.Session;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestColPartitionStoreExec {
  private String algorithm;

  private static TajoTestingCluster util;
  private static TajoConf conf;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static FileTablespace sm;
  private static Path testDir;
  private static Session session = LocalTajoTestingUtility.createDummySession();
  private static QueryContext defaultContext;

  private static TableDesc largeScore = null;

  private static MasterPlan masterPlan;


  private final static String CreateTableAsStmts =
    "CREATE TABLE score_part (deptname text, score int4, nullable text) PARTITION BY COLUMN (class text) " +
      "AS SELECT deptname, score, nullable, class from score_large";

  public TestColPartitionStoreExec(String algorithm) {
    this.algorithm = algorithm;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
      {"Hash"},
      {"Sort"},
    });
  }

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();

    util.startCatalogCluster();
    conf = util.getConfiguration();
    testDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/TestColPartitionStoreExec");
    sm = TablespaceManager.getLocalFs();
    catalog = util.getCatalogService();
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    catalog.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : FunctionLoader.findLegacyFunctions()) {
      catalog.createFunction(funcDesc);
    }

    defaultContext = LocalTajoTestingUtility.createDummyContext(conf);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog, TablespaceManager.getInstance());
    optimizer = new LogicalOptimizer(conf, catalog);
    masterPlan = new MasterPlan(LocalTajoTestingUtility.newQueryId(), null, null);

    createLargeScoreTable();
  }

  public static void createLargeScoreTable() throws IOException, TajoException {

    // Preparing a large table
    Path scoreLargePath = new Path(testDir, "score_large");
    CommonTestingUtil.cleanupTestDir(scoreLargePath.toString());

    Schema scoreSchmea = new Schema();
    scoreSchmea.addColumn("deptname", TajoDataTypes.Type.TEXT);
    scoreSchmea.addColumn("class", TajoDataTypes.Type.TEXT);
    scoreSchmea.addColumn("score", TajoDataTypes.Type.INT4);
    scoreSchmea.addColumn("nullable", TajoDataTypes.Type.TEXT);

    TableMeta scoreLargeMeta = CatalogUtil.newTableMeta("RAW", new KeyValueSet());
    Appender appender =  ((FileTablespace) TablespaceManager.getLocalFs())
      .getAppender(scoreLargeMeta, scoreSchmea, scoreLargePath);
    appender.enableStats();
    appender.init();
    largeScore = new TableDesc(
      CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "score_large"), scoreSchmea, scoreLargeMeta,
      scoreLargePath.toUri());

    VTuple tuple = new VTuple(scoreSchmea.size());
    int m = 0;
    for (int i = 1; i <= 30000; i++) {
      for (int k = 3; k < 5; k++) { // |{3,4}| = 2
        for (int j = 1; j <= 3; j++) { // |{1,2,3}| = 3
          tuple.put(
            new Datum[] {
              DatumFactory.createText("name_" + i), // name_1 ~ 5 (cad: // 5)
              DatumFactory.createText(k + "rd"), // 3 or 4rd (cad: 2)
              DatumFactory.createInt4(j), // 1 ~ 3
              k == 3 ? DatumFactory.createText("one") : NullDatum.get()});
          appender.addTuple(tuple);
          m++;
        }
      }
    }
    appender.flush();
    appender.close();
    largeScore.setStats(appender.getStats());
    catalog.createTable(largeScore);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public void testPartitionedStorePlanWithAlgorithm() throws IOException, TajoException {
    // Preparing working dir and input fragments
    FileFragment[] frags = FileTablespace.splitNG(conf, "default.score_large", largeScore.getMeta(),
      new Path(largeScore.getUri()), Integer.MAX_VALUE);
    TaskAttemptId id = LocalTajoTestingUtility.newTaskAttemptId(masterPlan);
    Path workDir = CommonTestingUtil.getTestDir(TajoTestingCluster.DEFAULT_TEST_DIRECTORY + "/testPartitionedStorePlanWithAlgorithm");

    // Setting session variables
    QueryContext queryContext = new QueryContext(conf, session);
    queryContext.setInt(SessionVars.MAX_OUTPUT_FILE_SIZE, 1);

    Expr context = analyzer.parse(CreateTableAsStmts);
    LogicalPlan plan = planner.createPlan(queryContext, context);
    LogicalNode rootNode = optimizer.optimize(plan);
    CreateTableNode createTableNode = (CreateTableNode)rootNode.getChild(0);

    // Preparing task context
    TaskAttemptContext ctx = new TaskAttemptContext(queryContext, id, new FileFragment[] { frags[0] }, workDir);
    ctx.setOutputPath(new Path(workDir, "part-01-000000"));

    // Set partition algorithm
    Enforcer enforcer = new Enforcer();
    if (algorithm.equalsIgnoreCase("hash")) {
      enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(),
        PlanProto.ColumnPartitionEnforcer.ColumnPartitionAlgorithm.HASH_PARTITION);
    } else {
      enforcer.enforceColumnPartitionAlgorithm(createTableNode.getPID(),
        PlanProto.ColumnPartitionEnforcer.ColumnPartitionAlgorithm.SORT_PARTITION);
    }

    ctx.setEnforcer(new Enforcer());
    // Executing CREATE TABLE PARTITION BY
    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    exec.init();
    exec.next();
    exec.close();

    FileSystem fs = sm.getFileSystem();
    FileStatus [] list = fs.listStatus(workDir);
    // checking the number of partitions
    assertEquals(2, list.length);

    List<Fragment> fragments = Lists.newArrayList();
    for (FileStatus status : list) {
      assertTrue(status.isDirectory());

      long fileVolumeSum = 0;
      FileStatus [] fileStatuses = fs.listStatus(status.getPath());
      for (FileStatus fileStatus : fileStatuses) {
        fileVolumeSum += fileStatus.getLen();
        fragments.add(new FileFragment("partition", fileStatus.getPath(), 0, fileStatus.getLen()));
      }
      assertTrue("checking the meaningfulness of test", fileVolumeSum > StorageUnit.MB && fileStatuses.length > 1);

      long expectedFileNum = (long) Math.ceil(fileVolumeSum / (float)StorageUnit.MB);
      assertEquals(expectedFileNum, fileStatuses.length);

      PartitionDescProto partition = ctx.getPartition(status.getPath().getName());
      assertNotNull(partition);
      assertEquals(fileVolumeSum, partition.getNumBytes());
    }

    TableMeta outputMeta = CatalogUtil.newTableMeta("TEXT");
    Scanner scanner = new MergeScanner(conf, rootNode.getOutSchema(), outputMeta, TUtil.newList(fragments));
    scanner.init();

    long rowNum = 0;
    while (scanner.next() != null) {
      rowNum++;
    }

    // checking the number of all written rows
    assertTrue(largeScore.getStats().getNumRows() == rowNum);

    assertEquals(2, ctx.getPartitions().size());

    scanner.close();
  }
}
