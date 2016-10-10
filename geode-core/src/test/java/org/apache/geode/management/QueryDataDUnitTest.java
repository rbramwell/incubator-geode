/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management;

import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.*;
import static org.apache.geode.cache.query.Utils.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.dunit.QueryUsingFunctionContextDUnitTest;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.BeanUtilFuncs;
import org.apache.geode.management.internal.cli.json.TypedJson;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.geode.test.dunit.rules.DistributedUseJacksonForJsonPathRule;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Distributed tests for {@link DistributedSystemMXBean#queryData(String, String, int)}.
 *
 * <pre>
 * Test Basic Json Strings for Partitioned Regions
 * Test Basic Json Strings for Replicated Regions
 * Test for all Region Types
 * Test for primitive types
 * Test for Nested Objects
 * Test for Enums
 * Test for collections
 * Test for huge collection
 * Test PDX types
 * Test different projects type e.g. SelectResult, normal bean etc..
 * Test Colocated Regions
 * Test for Limit ( both row count and Depth)
 * ORDER by orders
 * Test all attributes are covered in an complex type
 * </pre>
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class QueryDataDUnitTest extends ManagementTestBase {

  private static final int MAX_WAIT = 100 * 1000;
  private static final int COUNT_DESTINATION = 30;
  private static final int COUNT_FROM = 0;

  // PR 5 is co-located with 4
  private static final String PARTITIONED_REGION_NAME1 = "TestPartitionedRegion1";
  private static final String PARTITIONED_REGION_NAME2 = "TestPartitionedRegion2";
  private static final String PARTITIONED_REGION_NAME3 = "TestPartitionedRegion3";
  private static final String PARTITIONED_REGION_NAME4 = "TestPartitionedRegion4";
  private static final String PARTITIONED_REGION_NAME5 = "TestPartitionedRegion5";

  private static final String REPLICATED_REGION_NAME1 = "TestRepRegion";
  private static final String REPLICATED_REGION_NAME2 = "TestRepRegion2";
  private static final String REPLICATED_REGION_NAME3 = "TestRepRegion3";
  private static final String REPLICATED_REGION_NAME4 = "TestRepRegion4";

  private static final String LOCAL_REGION_NAME = "TestLocalRegion";

  private static final String[] QUERIES = new String[] {
    "select * from /" + PARTITIONED_REGION_NAME1 + " where ID>=0",
    "Select * from /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2 + " r2 where r1.ID = r2.ID",
    "Select * from /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2 + " r2 where r1.ID = r2.ID AND r1.status = r2.status",
    "Select * from /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2 + " r2, /" + PARTITIONED_REGION_NAME3 + " r3 where r1.ID = r2.ID and r2.ID = r3.ID",
    "Select * from /" + PARTITIONED_REGION_NAME1 + " r1, /" + PARTITIONED_REGION_NAME2 + " r2, /" + PARTITIONED_REGION_NAME3 + " r3  , /" + REPLICATED_REGION_NAME1 + " r4 where r1.ID = r2.ID and r2.ID = r3.ID and r3.ID = r4.ID",
    "Select * from /" + PARTITIONED_REGION_NAME4 + " r4, /" + PARTITIONED_REGION_NAME5 + " r5 where r4.ID = r5.ID"
  };

  private static final String[] QUERIES_FOR_REPLICATED = new String[] {
    "<trace> select * from /" + REPLICATED_REGION_NAME1 + " where ID>=0",
    "Select * from /" + REPLICATED_REGION_NAME1 + " r1, /" + REPLICATED_REGION_NAME2 + " r2 where r1.ID = r2.ID",
    "select * from /" + REPLICATED_REGION_NAME3 + " where ID>=0"
  };

  private static final String[] QUERIES_FOR_LIMIT = new String[] {
    "select * from /" + REPLICATED_REGION_NAME4
  };

  private DistributedMember member1;
  private DistributedMember member2;
  private DistributedMember member3;

  @Rule
  public DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule = new DistributedUseJacksonForJsonPathRule();

  @Override
  protected final void postSetUpManagementTestBase() throws Exception {
    initManagement(false);

    this.member1 = getMember(managedNode1);
    this.member2 = getMember(managedNode2);
    this.member3 = getMember(managedNode3);

    createRegionsInNodes();
    fillValuesInRegions();
  }

  @Test
  public void testQueryOnPartitionedRegion() throws Exception {
    this.managingNode.invoke("testQueryOnPartitionedRegion", () -> {
      //Cache cache = getCache();
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

      assertNotNull(bean);

      for (int i = 0; i < QUERIES.length; i++) {
        String jsonString = null;
        if (i == 0) {
          jsonString = bean.queryData(QUERIES[i], null, 10);
          assertThat("Query On Cluster should have result", jsonString.contains("result") && !jsonString.contains("No Data Found"), is(true));
        } else {
          jsonString = bean.queryData(QUERIES[i], member1.getId(), 10);
          assertThat("Query On Member should have member", jsonString.contains("result"), is(true));
          assertThat("Query On Member should have member", jsonString.contains("member"), is(true));
          assertThat("QUERIES[" + i + "]", jsonString, isJson(withJsonPath("$..result", anything())));
//          assertThat("QUERIES[" + i + "]", result,
//            isJson(withJsonPath("$..member",
//                                equalTo(JsonPath.compile(result)))));
//                                //equalTo(new JSONObject().put(String.class.getName(), member1.getId())))));

          //System.out.println("KIRK: " + JsonPath.read(jsonString, "$.result.*"));
          // TODO: System.out.println("KIRK: " + JsonPath.read(jsonString, "$['result']['member']"));

        }
        assertIsValidJson(jsonString);
      }
    });
  }

  @Test
  public void testQueryOnReplicatedRegion() throws Exception {
    this.managingNode.invoke("testQueryOnReplicatedRegion", () -> {
      Cache cache = getCache();
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      assertNotNull(bean);

      for (int i = 0; i < QUERIES_FOR_REPLICATED.length; i++) {
        String jsonString = bean.queryData(QUERIES_FOR_REPLICATED[i], null, 10);
        if (i == 0) {
          assertThat("Query On Cluster should have result", jsonString.contains("result") && !jsonString.contains("No Data Found"), is(true));
        } else {
          assertThat("Join on Replicated did not work.", jsonString.contains("result"), is(true));
        }
        assertIsValidJson(jsonString);
      }
    });
  }

  @Test
  public void testMemberWise() throws Exception {
    this.managingNode.invoke("testMemberWise", () -> {
      Cache cache = getCache();
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      assertNotNull(bean);

      byte[] bytes = bean.queryDataForCompressedResult(QUERIES_FOR_REPLICATED[0], member1.getId() + "," + member2.getId(), 2);
      String jsonString = BeanUtilFuncs.decompress(bytes);

      assertIsValidJson(jsonString);
    });
  }

  @Test
  public void testLimitForQuery() throws Exception {
    managedNode1.invoke("putBigInstances", () -> putBigInstances(REPLICATED_REGION_NAME4));

    managingNode.invoke("testLimitForQuery", () -> {
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      assertNotNull(bean);

      // Query With Default values
      assertEquals(TypedJson.DEFAULT_COLLECTION_ELEMENT_LIMIT, bean.getQueryCollectionsDepth());
      assertEquals(ManagementConstants.DEFAULT_QUERY_LIMIT, bean.getQueryResultSetLimit());

      String jsonString = bean.queryData(QUERIES_FOR_LIMIT[0], null, 0);

      assertIsValidJson(jsonString);
      assertThat(jsonString.contains("result") && !jsonString.contains("No Data Found"), is(true));
      assertTrue(jsonString.contains("BigColl_1_ElemenNo_"));

      JSONObject jsonObject = new JSONObject(jsonString);
      JSONArray jsonArray = jsonObject.getJSONArray("result");
      assertEquals(ManagementConstants.DEFAULT_QUERY_LIMIT, jsonArray.length());

      // Get the first element
      JSONArray jsonArray1 = jsonArray.getJSONArray(0);

      // Get the ObjectValue
      JSONObject collectionObject = (JSONObject) jsonArray1.get(1);
      assertEquals(100, collectionObject.length());

      // Query With Override Values
      int newQueryCollectionDepth = 150;
      int newQueryResultSetLimit = 500;
      bean.setQueryCollectionsDepth(newQueryCollectionDepth);
      bean.setQueryResultSetLimit(newQueryResultSetLimit);

      assertEquals(newQueryCollectionDepth, bean.getQueryCollectionsDepth());
      assertEquals(newQueryResultSetLimit, bean.getQueryResultSetLimit());

      jsonString = bean.queryData(QUERIES_FOR_LIMIT[0], null, 0);

      assertIsValidJson(jsonString);
      assertThat("Query On Cluster should have result", jsonString.contains("result") && !jsonString.contains("No Data Found"), is(true));

      jsonObject = new JSONObject(jsonString);
      assertTrue(jsonString.contains("BigColl_1_ElemenNo_"));

      jsonArray = jsonObject.getJSONArray("result");
      assertEquals(newQueryResultSetLimit, jsonArray.length());

      // Get the first element
      jsonArray1 = jsonArray.getJSONArray(0);

      // Get the ObjectValue
      collectionObject = (JSONObject) jsonArray1.get(1);
      assertEquals(newQueryCollectionDepth, collectionObject.length());
    });
  }

  private static String generateJson(String key, String value) throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(key, value);
    return jsonObject.toString();
  }

  @Test
  public void testErrors() throws Exception {
    this.managingNode.invoke("Test Error", () -> {
      //Cache cache = getCache();
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

      String invalidQuery = "Select * from " + PARTITIONED_REGION_NAME1;
      String invalidQueryResult = bean.queryData(invalidQuery, null, 2);
      assertThat(invalidQueryResult,
        isJson(withJsonPath("$.message", equalTo(ManagementStrings.QUERY__MSG__INVALID_QUERY.toLocalizedString("Region mentioned in query probably missing /")))));

      String regionsNotFoundQuery = "Select * from /PartitionedRegionName9 r1, PARTITIONED_REGION_NAME2 r2 where r1.ID = r2.ID";
      String regionsNotFoundResult = bean.queryData(regionsNotFoundQuery, null, 2);
      assertThat(regionsNotFoundResult,
        isJson(withJsonPath("$.message", equalTo(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND.toLocalizedString("/PartitionedRegionName9")))));

      String region = "testTemp";
      String regionsNotFoundOnMembersQuery = "Select * from /" + region;
      RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory.create(region);
      String regionsNotFoundOnMembersResult = bean.queryData(regionsNotFoundOnMembersQuery, member1.getId(), 2);
      assertThat(regionsNotFoundOnMembersResult,
        isJson(withJsonPath("$.message", equalTo(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBERS.toLocalizedString("/" + region)))));

      String joinMissingMembersQuery = QUERIES[1];
      String joinMissingMembersResult = bean.queryData(joinMissingMembersQuery, null, 2);
      assertThat(joinMissingMembersResult,
        isJson(withJsonPath("$.message", equalTo(ManagementStrings.QUERY__MSG__JOIN_OP_EX.toLocalizedString()))));
    });
  }

  @Test
  public void testNormalRegions() throws Exception {
    this.managingNode.invoke("Test Error", () -> {
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      assertNotNull(bean);

      final String testNormal = "testNormal";
      final String testTemp = "testTemp";

      final String testSNormal = "testSNormal"; // to Reverse order of regions while getting Random region in QueryDataFunction
      final String testATemp = "testATemp";

      Cache cache = getCache();
      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);
      regionFactory.create(testNormal);
      regionFactory.create(testSNormal);

      Region region = cache.getRegion("/" + testNormal);
      assertTrue(region.getAttributes().getDataPolicy() == DataPolicy.NORMAL);

      RegionFactory regionFactory1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory1.create(testTemp);
      regionFactory1.create(testATemp);
      String query1 = "Select * from /testTemp r1,/testNormal r2 where r1.ID = r2.ID";
      String query2 = "Select * from /testSNormal r1,/testATemp r2 where r1.ID = r2.ID";
      String query3 = "Select * from /testSNormal";

      bean.queryDataForCompressedResult(query1, null, 2);
      bean.queryDataForCompressedResult(query2, null, 2);
      bean.queryDataForCompressedResult(query3, null, 2);

      // TODO: assert results of queryDataForCompressedResult?
    });
  }

  @Test
  public void testRegionsLocalDataSet() throws Exception {
    final String PartitionedRegionName6 = "LocalDataSetTest";

    final String[] valArray1 = new String[] { "val1", "val2", "val3" };
    final String[] valArray2 = new String[] { "val4", "val5", "val6" };

    this.managedNode1.invoke("testRegionsLocalDataSet:Create Region", () -> {
      Cache cache = getCache();
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();

      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(1);
      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
      }
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(partitionAttributesFactory.create());

      Region region = regionFactory.create(PartitionedRegionName6);

      for (int i = 0; i < valArray1.length; i++) {
        region.put(new Date(2013, 1, i + 5), valArray1[i]);
      }
    });

    this.managedNode2.invoke("testRegionsLocalDataSet: Create Region", () -> {
      Cache cache = getCache();
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();

      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(2);
      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
      }
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(partitionAttributesFactory.create());

      Region region = regionFactory.create(PartitionedRegionName6);

      for (int i = 0; i < valArray2.length; i++) {
        region.put(new Date(2013, 5, i + 5), valArray2[i]);
      }
    });

    this.managedNode3.invoke("testRegionsLocalDataSet: Create Region", () -> {
      Cache cache = getCache();
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();

      partitionAttributesFactory.setRedundantCopies(2).setTotalNumBuckets(12);

      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(3);
      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fixedPartitionAttributes);
      }
      partitionAttributesFactory.setPartitionResolver(new SingleHopQuarterPartitionResolver());

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(partitionAttributesFactory.create());

      Region region = regionFactory.create(PartitionedRegionName6);
    });

    final List<String> member1RealData = managedNode1.invoke(() -> getLocalDataSet(PartitionedRegionName6));
    final List<String> member2RealData = managedNode2.invoke(() -> getLocalDataSet(PartitionedRegionName6));
    final List<String> member3RealData = managedNode3.invoke(() -> getLocalDataSet(PartitionedRegionName6));

    this.managingNode.invoke("testRegionsLocalDataSet", () -> {
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
      assertNotNull(bean);

      DistributedRegionMXBean regionMBean = MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName6, 3);

      Wait.waitForCriterion(new WaitCriterion() {
        @Override
        public String description() {
          return "Waiting for all entries to get reflected at managing node";
        }
        @Override
        public boolean done() {
          return regionMBean.getSystemRegionEntryCount() == (valArray1.length + valArray2.length);
        }
      }, MAX_WAIT, 1000, true);

      String query = "Select * from /" + PartitionedRegionName6;

      String member1Result = bean.queryData(query, member1.getId(), 0);
      assertIsValidJson(member1Result);

      String member2Result = bean.queryData(query, member2.getId(), 0);
      assertIsValidJson(member2Result);

      String member3Result = bean.queryData(query, member3.getId(), 0);
      assertIsValidJson(member3Result);

      for (String val : member1RealData) {
        assertTrue(member1Result.contains(val));
      }
      for (String val : member2RealData) {
        assertTrue(member2Result.contains(val));
      }
      assertTrue(member3Result.contains("No Data Found"));
    });
  }

  private static void assertIsValidJson(final String jsonString) throws JSONException {
    assertThat(jsonString, isJson());
    assertThat(jsonString, hasJsonPath("$.result"));
    assertThat(new JSONObject(jsonString), is(notNullValue()));
  }

  private void putDataInRegion(final String regionName,
                               final Object[] portfolio,
                               final int from,
                               final int to) {
    Cache cache = CacheFactory.getAnyInstance();
    Region region = cache.getRegion(regionName);
    for (int i = from; i < to; i++) {
      region.put(new Integer(i), portfolio[i]);
    }
  }

  private void fillValuesInRegions() {
    // Create common Portfolios and NewPortfolios
    final Portfolio[] portfolio = createPortfoliosAndPositions(COUNT_DESTINATION);

    // Fill local region
    managedNode1.invoke(() -> putDataInRegion(LOCAL_REGION_NAME, portfolio, COUNT_FROM, COUNT_DESTINATION));

    // Fill replicated region
    managedNode1.invoke(() -> putDataInRegion(REPLICATED_REGION_NAME1, portfolio, COUNT_FROM, COUNT_DESTINATION));
    managedNode2.invoke(() -> putDataInRegion(REPLICATED_REGION_NAME2, portfolio, COUNT_FROM, COUNT_DESTINATION));

    // Fill Partition Region
    managedNode1.invoke(() -> putDataInRegion(PARTITIONED_REGION_NAME1, portfolio, COUNT_FROM, COUNT_DESTINATION));
    managedNode1.invoke(() -> putDataInRegion(PARTITIONED_REGION_NAME2, portfolio, COUNT_FROM, COUNT_DESTINATION));
    managedNode1.invoke(() -> putDataInRegion(PARTITIONED_REGION_NAME3, portfolio, COUNT_FROM, COUNT_DESTINATION));
    managedNode1.invoke(() -> putDataInRegion(PARTITIONED_REGION_NAME4, portfolio, COUNT_FROM, COUNT_DESTINATION));
    managedNode1.invoke(() -> putDataInRegion(PARTITIONED_REGION_NAME5, portfolio, COUNT_FROM, COUNT_DESTINATION));

    managedNode1.invoke(() -> putPdxInstances(REPLICATED_REGION_NAME3));
  }

  private void putPdxInstances(final String regionName) throws CacheException {
    PdxInstanceFactory pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    Region region = getCache().getRegion(regionName);
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "IBM");
    PdxInstance pdxInstance = pdxInstanceFactory.create();
    region.put("IBM", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pdxInstanceFactory.writeInt("ID", 222);
    pdxInstanceFactory.writeString("status", "inactive");
    pdxInstanceFactory.writeString("secId", "YHOO");
    pdxInstance = pdxInstanceFactory.create();
    region.put("YHOO", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pdxInstanceFactory.writeInt("ID", 333);
    pdxInstanceFactory.writeString("status", "active");
    pdxInstanceFactory.writeString("secId", "GOOGL");
    pdxInstance = pdxInstanceFactory.create();
    region.put("GOOGL", pdxInstance);

    pdxInstanceFactory = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pdxInstanceFactory.writeInt("ID", 111);
    pdxInstanceFactory.writeString("status", "inactive");
    pdxInstanceFactory.writeString("secId", "VMW");
    pdxInstance = pdxInstanceFactory.create();
    region.put("VMW", pdxInstance);
  }

  private void putBigInstances(final String regionName) {
    Region region = getCache().getRegion(regionName);

    for (int i = 0; i < 1200; i++) {
      List<String> bigCollection = new ArrayList<>();
      for (int j = 0; j < 200; j++) {
        bigCollection.add("BigColl_1_ElemenNo_" + j);
      }
      region.put("BigColl_1_" + i, bigCollection);
    }
  }

  private void createRegionsInNodes() throws InterruptedException {

    // Create local Region on servers
    managedNode1.invoke(() -> QueryUsingFunctionContextDUnitTest.createLocalRegion());

    // Create ReplicatedRegion on servers
    managedNode1.invoke(() -> QueryUsingFunctionContextDUnitTest.createReplicatedRegion());
    managedNode2.invoke(() -> QueryUsingFunctionContextDUnitTest.createReplicatedRegion());
    managedNode3.invoke(() -> QueryUsingFunctionContextDUnitTest.createReplicatedRegion());

    createDistributedRegion(managedNode2, REPLICATED_REGION_NAME2);
    createDistributedRegion(managedNode1, REPLICATED_REGION_NAME3);
    createDistributedRegion(managedNode1, REPLICATED_REGION_NAME4);

    // Create two colocated PartitionedRegions On Servers.
    managedNode1.invoke(() -> QueryUsingFunctionContextDUnitTest.createColocatedPR());
    managedNode2.invoke(() -> QueryUsingFunctionContextDUnitTest.createColocatedPR());
    managedNode3.invoke(() -> QueryUsingFunctionContextDUnitTest.createColocatedPR());

    this.managingNode.invoke("Wait for all Region Proxies to get replicated", () -> {
      Cache cache = getCache();
      SystemManagementService service = (SystemManagementService) getManagementService();
      DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

      MBeanUtil.getDistributedRegionMbean("/" + PARTITIONED_REGION_NAME1, 3);
      MBeanUtil.getDistributedRegionMbean("/" + PARTITIONED_REGION_NAME2, 3);
      MBeanUtil.getDistributedRegionMbean("/" + PARTITIONED_REGION_NAME3, 3);
      MBeanUtil.getDistributedRegionMbean("/" + PARTITIONED_REGION_NAME4, 3);
      MBeanUtil.getDistributedRegionMbean("/" + PARTITIONED_REGION_NAME5, 3);
      MBeanUtil.getDistributedRegionMbean("/" + REPLICATED_REGION_NAME1, 3);
      MBeanUtil.getDistributedRegionMbean("/" + REPLICATED_REGION_NAME2, 1);
      MBeanUtil.getDistributedRegionMbean("/" + REPLICATED_REGION_NAME3, 1);
      MBeanUtil.getDistributedRegionMbean("/" + REPLICATED_REGION_NAME4, 1);
    });
  }

  private static List<String> getLocalDataSet(final String region) {
    PartitionedRegion partitionedRegion = PartitionedRegionHelper.getPartitionedRegion(region, GemFireCacheImpl.getExisting());
    Set<BucketRegion> localPrimaryBucketRegions = partitionedRegion.getDataStore().getAllLocalPrimaryBucketRegions();
    List<String> allPrimaryValues = new ArrayList<>();

    for (BucketRegion brRegion : localPrimaryBucketRegions) {
      for (Object obj : brRegion.values()) {
        allPrimaryValues.add((String) obj);
      }
    }

    return allPrimaryValues;
  }

  /**
   * creates a Fixed Partition List to be used for Fixed Partition Region
   *
   * @param primaryIndex index for each fixed partition
   */
  private static List<FixedPartitionAttributes> createFixedPartitionList(final int primaryIndex) {
    List<FixedPartitionAttributes> fixedPartitionAttributesList = new ArrayList<>();
    if (primaryIndex == 1) {
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q1", true, 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 2) {
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q2", true, 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q3", 3));
    }
    if (primaryIndex == 3) {
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q1", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q2", 3));
      fixedPartitionAttributesList.add(FixedPartitionAttributes.createFixedPartition("Q3", true, 3));
    }
    return fixedPartitionAttributesList;
  }
}
