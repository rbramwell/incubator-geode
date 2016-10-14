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

import static com.jayway.awaitility.Awaitility.*;
import static org.apache.geode.cache.Region.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TestObjectSizerImpl;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.partitioned.fixed.SingleHopQuarterPartitionResolver;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class checks and verifies various data and operations exposed through
 * RegionMXBean interface.
 * <p>
 * Goal of the Test : RegionMBean gets created once region is created. Data like
 * Region Attributes data and stats are of proper value
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class RegionManagementDUnitTest extends ManagementTestBase {
//public class RegionManagementDUnitTest extends JUnit4DistributedTestCase {

  private static final int MAX_WAIT_MILLIS = 70 * 1000;

  private static final String REGION_NAME = "MANAGEMENT_TEST_REGION";
  private static final String PARTITIONED_REGION_NAME = "MANAGEMENT_PAR_REGION";
  private static final String FIXED_PR_NAME = "MANAGEMENT_FIXED_PR";
  private static final String LOCAL_REGION_NAME = "TEST_LOCAL_REGION";
  private static final String LOCAL_SUB_REGION_NAME = "TEST_LOCAL_SUB_REGION";

  private static final String REGION_PATH = SEPARATOR + REGION_NAME;
  private static final String PARTITIONED_REGION_PATH = SEPARATOR + PARTITIONED_REGION_NAME;
  private static final String FIXED_PR_PATH = SEPARATOR + FIXED_PR_NAME;
  private static final String LOCAL_SUB_REGION_PATH = SEPARATOR + LOCAL_REGION_NAME + SEPARATOR + LOCAL_SUB_REGION_NAME;

  // fields used in managedNode VMs
  private static Region fixedPartitionedRegion;

  // this.managerVM is VM 0
  // managedNodes are VMs 1-3

  private VM managerVM;
  private VM[] memberVMs;

  @Before
  public void before() throws Exception {
    this.managerVM = getHost(0).getVM(0);

    this.memberVMs = new VM[3];
    this.memberVMs[0] = getHost(0).getVM(1);
    this.memberVMs[1] = getHost(0).getVM(2);
    this.memberVMs[2] = getHost(0).getVM(3);
  }

  @After
  public void after() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Creates and check a Distributed Region
   */
  @Test
  public void testDistributedRegion() throws Exception {
    initManagement(false);

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM); // TODO: what does this do for us?

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion(memberVM, REGION_NAME);
      verifyReplicateRegionAfterCreate(memberVM);
    }

    verifyRemoteDistributedRegion(this.managerVM, 3);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
      verifyReplicatedRegionAfterClose(memberVM);
    }

    verifyProxyCleanup(this.managerVM);
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Created and check a Partitioned Region
   */
  @Test
  public void testPartitionedRegion() throws Exception {
    initManagement(false);

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM); // TODO: what does this do for us?

    for (VM memberVM : this.memberVMs) {
      createPartitionRegion(memberVM, PARTITIONED_REGION_NAME);
      verifyPartitionRegionAfterCreate(memberVM);
    }

    verifyRemotePartitionRegion(this.managerVM);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, PARTITIONED_REGION_PATH);
      verifyPartitionRegionAfterClose(memberVM);
    }
  }

  /**
   * Tests all Region MBean related Management APIs
   * <p>
   * a) Notification propagated to member MBean while a region is created
   * <p>
   * b) Creates and check a Fixed Partitioned Region
   */
  @Test
  public void testFixedPRRegionMBean() throws Exception {
    initManagement(false);

    // Adding notification listener for remote cache memberVMs
    addMemberNotificationListener(this.managerVM); // TODO: what does this do for us?

    int primaryIndex = 0;
    for (VM memberVM : this.memberVMs) {
      List<FixedPartitionAttributes> fixedPartitionAttributesList = createFixedPartitionList(primaryIndex + 1);
      memberVM.invoke(() -> createFixedPartitionRegion(fixedPartitionAttributesList));
      primaryIndex++;
    }

//    // TODO: Workaround for bug 46683. Reenable validation when bug is fixed.
//    verifyRemoteFixedPartitionRegion(this.managerVM);
//
//    for (VM vm : this.memberVMs) {
//      closeFixedPartitionRegion(vm);
//    }
  }

  /**
   * Tests a Distributed Region at Managing Node side
   * while region is created in a member node asynchronously.
   */
  @Test
  public void testRegionAggregate() throws Exception {
    initManagement(true);

    // Adding notification listener for remote cache memberVMs
    addDistributedSystemNotificationListener(this.managerVM); // TODO: what does this do for us?

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion(memberVM, REGION_NAME);
    }

    verifyDistributedMBean(this.managerVM, 3);
    createDistributedRegion(this.managerVM, REGION_NAME);
    verifyDistributedMBean(this.managerVM, 4);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
    }

    verifyProxyCleanup(this.managerVM);

    verifyDistributedMBean(this.managerVM, 1);
    closeRegion(this.managerVM, REGION_PATH);
    verifyDistributedMBean(this.managerVM, 0);
  }

  @Test
  public void testNavigationAPIS() throws Exception {
    initManagement(true);

    for (VM memberVM : this.memberVMs) {
      createDistributedRegion(memberVM, REGION_NAME);
      createPartitionRegion(memberVM, PARTITIONED_REGION_NAME);
    }

    createDistributedRegion(this.managerVM, REGION_NAME);
    createPartitionRegion(this.managerVM, PARTITIONED_REGION_NAME);
    List<String> memberIds = new ArrayList<>();

    for (VM memberVM : this.memberVMs) {
      memberIds.add(getMemberId(memberVM));
    }

    verifyNavigationApis(this.managerVM, memberIds);

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, REGION_PATH);
    }
    closeRegion(this.managerVM, REGION_PATH);
  }


  @Test
  public void testSubRegions() throws Exception {
    initManagement(false);

    for (VM memberVM : this.memberVMs) {
      createLocalRegion(memberVM, LOCAL_REGION_NAME);
      createSubRegion(memberVM, LOCAL_REGION_NAME, LOCAL_SUB_REGION_NAME);
    }

    for (VM memberVM : this.memberVMs) {
      verifySubRegions(memberVM, LOCAL_SUB_REGION_PATH);
    }

    for (VM memberVM : this.memberVMs) {
      closeRegion(memberVM, LOCAL_REGION_NAME);
      verifyNullRegions(memberVM, LOCAL_SUB_REGION_NAME);
    }
  }

  @Test
  public void testSpecialRegions() throws Exception {
    initManagement(false);
    createSpecialRegion(this.memberVMs[0]);
    DistributedMember member = getMember(this.memberVMs[0]);
    verifySpecialRegion(this.managerVM);
  }

  @Test
  public void testLruStats() throws Exception {
    initManagement(false);
    for (VM memberVM : this.memberVMs) {
      createDiskRegion(memberVM);
    }
    verifyEntrySize(this.managerVM, 3);
  }

  private void closeRegion(final VM anyVM, final String regionPath) {
    anyVM.invoke("closeRegion", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Region region = cache.getRegion(regionPath);
      region.close();
    });
  }

  private void createSpecialRegion(final VM memberVM) throws Exception {
    memberVM.invoke("createSpecialRegion", () -> {
      Cache cache = getCache();
      AttributesFactory attributesFactory = new AttributesFactory();
      attributesFactory.setValueConstraint(Portfolio.class);
      RegionAttributes regionAttributes = attributesFactory.create();

      cache.createRegion("p-os", regionAttributes);
      cache.createRegion("p_os", regionAttributes);
    });
  }

  private void verifySpecialRegion(final VM managerVM) throws Exception {
    managerVM.invoke("verifySpecialRegion", () -> {
      MBeanUtil.getDistributedRegionMbean("/p-os", 1); // TODO: do something?
      MBeanUtil.getDistributedRegionMbean("/p_os", 1);
    });
  }

  private void createDiskRegion(final VM memberVM) throws Exception {
    memberVM.invoke("createDiskRegion", () -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(20, new TestObjectSizerImpl(), EvictionAction.LOCAL_DESTROY));

      Region region = getCache().createRegion(REGION_NAME, factory.create());

      LRUStatistics lruStats = ((AbstractRegion) region).getEvictionController().getLRUHelper().getStats();
      assertNotNull(lruStats);

      RegionMXBean regionMXBean = managementService.getLocalRegionMBean(REGION_PATH);
      assertNotNull(regionMXBean);

      int total;
      for (total = 0; total < 10000; total++) { // TODO: what is this?
        int[] array = new int[250];
        array[0] = total;
        region.put(new Integer(total), array);
      }
      assertTrue(regionMXBean.getEntrySize() > 0);
    });
  }

  private void verifyEntrySize(final VM managerVM, final int expectedMembers) throws Exception {
    managerVM.invoke("verifyEntrySize", () -> {
      DistributedRegionMXBean distributedRegionMXBean = MBeanUtil.getDistributedRegionMbean(REGION_PATH, expectedMembers);
      assertNotNull(distributedRegionMXBean);
      assertTrue(distributedRegionMXBean.getEntrySize() > 0);
    });
  }

  private void verifySubRegions(final VM memberVM, final String subRegionPath) throws Exception {
    memberVM.invoke("verifySubRegions", () -> {
      RegionMXBean regionMXBean = managementService.getLocalRegionMBean(subRegionPath);
      assertNotNull(regionMXBean);
    });
  }

  private void verifyNullRegions(final VM memberVM, final String subRegionPath) throws Exception {
    memberVM.invoke("verifyNullRegions", () -> {
      RegionMXBean regionMXBean = managementService.getLocalRegionMBean(subRegionPath);
      assertNull(regionMXBean);
    });
  }

  private void verifyNavigationApis(final VM managerVM, final List<String> memberIds) {
    managerVM.invoke("verifyNavigationApis", () -> {
      ManagementService service = getManagementService();
      assertNotNull(service.getDistributedSystemMXBean());

      waitForAllMembers(4);

      DistributedSystemMXBean distributedSystemMXBean = service.getDistributedSystemMXBean();
      assertTrue(distributedSystemMXBean.listDistributedRegionObjectNames().length == 2);

      assertNotNull(distributedSystemMXBean.fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH));
      assertNotNull(distributedSystemMXBean.fetchDistributedRegionObjectName(REGION_PATH));

      ObjectName actualName = distributedSystemMXBean.fetchDistributedRegionObjectName(PARTITIONED_REGION_PATH);
      ObjectName expectedName = MBeanJMXAdapter.getDistributedRegionMbeanName(PARTITIONED_REGION_PATH);
      assertEquals(expectedName, actualName);

      actualName = distributedSystemMXBean.fetchDistributedRegionObjectName(REGION_PATH);
      expectedName = MBeanJMXAdapter.getDistributedRegionMbeanName(REGION_PATH);
      assertEquals(expectedName, actualName);

      for (String memberId : memberIds) {
        ObjectName memberMBeanName = MBeanJMXAdapter.getMemberMBeanName(memberId);
        waitForProxy(memberMBeanName, MemberMXBean.class);

        ObjectName[] regionMBeanNames = distributedSystemMXBean.fetchRegionObjectNames(memberMBeanName);
        assertNotNull(regionMBeanNames);
        assertTrue(regionMBeanNames.length == 2);

        List<ObjectName> listOfNames = Arrays.asList(regionMBeanNames);

        expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId, PARTITIONED_REGION_PATH);
        assertTrue(listOfNames.contains(expectedName));

        expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId, REGION_PATH);
        assertTrue(listOfNames.contains(expectedName));
      }

      for (String memberId : memberIds) {
        ObjectName memberMBeanName = MBeanJMXAdapter.getMemberMBeanName(memberId);
        waitForProxy(memberMBeanName, MemberMXBean.class);

        expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId, PARTITIONED_REGION_PATH);
        waitForProxy(expectedName, RegionMXBean.class);

        actualName = distributedSystemMXBean.fetchRegionObjectName(memberId, PARTITIONED_REGION_PATH);
        assertEquals(expectedName, actualName);

        expectedName = MBeanJMXAdapter.getRegionMBeanName(memberId, REGION_PATH);
        waitForProxy(expectedName, RegionMXBean.class);

        actualName = distributedSystemMXBean.fetchRegionObjectName(memberId, REGION_PATH);
        assertEquals(expectedName, actualName);
      }
    });
  }

  /**
   * Invoked in controller VM
   */
  private List<FixedPartitionAttributes> createFixedPartitionList(final int primaryIndex) {
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

  /**
   * Invoked in member VMs
   */
  private static void createFixedPartitionRegion(final List<FixedPartitionAttributes> fixedPartitionAttributesList) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    SystemManagementService service = (SystemManagementService) getManagementService();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();

    paf.setRedundantCopies(2).setTotalNumBuckets(12);
    for (FixedPartitionAttributes fpa : fixedPartitionAttributesList) {
      paf.addFixedPartitionAttributes(fpa);
    }
    paf.setPartitionResolver(new SingleHopQuarterPartitionResolver());

    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(paf.create());

    fixedPartitionedRegion = cache.createRegion(FIXED_PR_NAME, attr.create());
    assertNotNull(fixedPartitionedRegion);

    RegionMXBean bean = service.getLocalRegionMBean(FIXED_PR_PATH);
    RegionAttributes regAttrs = fixedPartitionedRegion.getAttributes();

    PartitionAttributesData parData = bean.listPartitionAttributes();

    assertPartitionData(regAttrs, parData);

    FixedPartitionAttributesData[] fixedPrData = bean.listFixedPartitionAttributes();

    assertNotNull(fixedPrData);

    assertEquals(3, fixedPrData.length);
    for (int i = 0; i < fixedPrData.length; i++) {
      //LogWriterUtils.getLogWriter().info("<ExpectedString> Fixed PR Data is " + fixedPrData[i] + "</ExpectedString> ");
    }
  }

  /**
   * Invoked in manager VM
   *
   * TODO: unused method
   */
  private void verifyRemoteFixedPartitionRegion(final VM vm) throws Exception {
    vm.invoke("Verify Partition region", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Set<DistributedMember> otherMemberSet = cache.getDistributionManager().getOtherNormalDistributionManagerIds();

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean bean = MBeanUtil.getRegionMbeanProxy(member, FIXED_PR_PATH);

        PartitionAttributesData data = bean.listPartitionAttributes();
        assertNotNull(data);

        FixedPartitionAttributesData[] fixedPrData = bean.listFixedPartitionAttributes();
        assertNotNull(fixedPrData);
        assertEquals(3, fixedPrData.length);

        for (int i = 0; i < fixedPrData.length; i++) {
          //LogWriterUtils.getLogWriter().info("<ExpectedString> Remote PR Data is " + fixedPrData[i] + "</ExpectedString> ");
        }
      }
    });
  }

  private void addMemberNotificationListener(final VM managerVM) {
    managerVM.invoke("addMemberNotificationListener", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Set<DistributedMember> otherMemberSet = cache.getDistributionManager().getOtherNormalDistributionManagerIds();

      SystemManagementService service = (SystemManagementService) getManagementService();

      for (DistributedMember member : otherMemberSet) {
        MemberNotificationListener listener = new MemberNotificationListener();
        ObjectName memberMBeanName = service.getMemberMBeanName(member);
        Set<ObjectName> names = service.queryMBeanNames(member);
        if (names != null) {
          for (ObjectName name : names) {
            //LogWriterUtils.getLogWriter().info("<ExpectedString> ObjectNames arr" + name + "</ExpectedString> ");
          }
        }

        waitForProxy(memberMBeanName, MemberMXBean.class);

        ManagementFactory.getPlatformMBeanServer().addNotificationListener(memberMBeanName, listener, null, null);
      }
    });
  }

  /**
   * Add a Notification listener to DistributedSystemMBean which should gather
   * all the notifications which are propagated through all individual
   * MemberMBeans Hence Region created/destroyed should be visible to this
   * listener
   */
  private void addDistributedSystemNotificationListener(final VM managerVM) {
    managerVM.invoke("addDistributedSystemNotificationListener", () -> {
      DistributedSystemNotificationListener listener = new DistributedSystemNotificationListener();
      ObjectName systemMBeanName = MBeanJMXAdapter.getDistributedSystemName();
      ManagementFactory.getPlatformMBeanServer().addNotificationListener(systemMBeanName, listener, null, null);
    });
  }

  private void verifyProxyCleanup(final VM managerVM) {
    managerVM.invoke("verifyProxyCleanup", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Set<DistributedMember> otherMemberSet = cache.getDistributionManager().getOtherNormalDistributionManagerIds();

      final SystemManagementService service = (SystemManagementService) getManagementService();

      for (final DistributedMember member : otherMemberSet) {
        String alias = "Waiting for the proxy to get deleted at managing node";
        await(alias).atMost(MAX_WAIT_MILLIS, TimeUnit.MILLISECONDS).until(() -> assertNull(service.getMBeanProxy(service.getRegionMBeanName(member, REGION_PATH), RegionMXBean.class)));
      }
    });
  }

  private void verifyRemoteDistributedRegion(final VM managerVM, final int expectedMembers) throws Exception {
    managerVM.invoke("verifyRemoteDistributedRegion", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Set<DistributedMember> otherMemberSet = cache.getDistributionManager().getOtherNormalDistributionManagerIds();

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean bean = MBeanUtil.getRegionMbeanProxy(member, REGION_PATH);
        assertNotNull(bean);

        RegionAttributesData data = bean.listRegionAttributes();
        assertNotNull(data);

        MembershipAttributesData membershipData = bean.listMembershipAttributes();
        EvictionAttributesData evictionData = bean.listEvictionAttributes();
        assertNotNull(membershipData);
        assertNotNull(evictionData);

        //LogWriterUtils.getLogWriter().info("<ExpectedString> Membership Data is " + membershipData.toString() + "</ExpectedString> ");
        //LogWriterUtils.getLogWriter().info("<ExpectedString> Eviction Data is " + membershipData.toString() + "</ExpectedString> ");
      }

      DistributedRegionMXBean bean = null;
      bean = MBeanUtil.getDistributedRegionMbean(REGION_PATH, expectedMembers);

      assertNotNull(bean);
      assertEquals(REGION_PATH, bean.getFullPath());
    });
  }

  private void verifyDistributedMBean(final VM managerVM, final int expectedMembers) {
    managerVM.invoke("verifyDistributedMBean", () -> {
      final ManagementService service = getManagementService();

      if (expectedMembers == 0) {
        String alias = "Waiting for the proxy to get deleted at managing node";
        await(alias).atMost(MAX_WAIT_MILLIS, TimeUnit.MILLISECONDS).until(() -> assertNull(service.getDistributedRegionMXBean(REGION_PATH)));
        return;
      }

      DistributedRegionMXBean bean = MBeanUtil.getDistributedRegionMbean(REGION_PATH, expectedMembers);

      assertNotNull(bean);
      assertEquals(REGION_PATH, bean.getFullPath());
      assertEquals(expectedMembers, bean.getMemberCount());
      assertEquals(expectedMembers, bean.getMembers().length);

      // Check Stats related Data
      //LogWriterUtils.getLogWriter().info("<ExpectedString> CacheListenerCallsAvgLatency is " + bean.getCacheListenerCallsAvgLatency() + "</ExpectedString> ");
      //LogWriterUtils.getLogWriter().info("<ExpectedString> CacheWriterCallsAvgLatency is " + bean.getCacheWriterCallsAvgLatency() + "</ExpectedString> ");
      //LogWriterUtils.getLogWriter().info("<ExpectedString> CreatesRate is " + bean.getCreatesRate() + "</ExpectedString> ");
    });
  }

  private void verifyRemotePartitionRegion(final VM managerVM) throws Exception {
    managerVM.invoke("verifyRemotePartitionRegion", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Set<DistributedMember> otherMemberSet = cache.getDistributionManager().getOtherNormalDistributionManagerIds();

      for (DistributedMember member : otherMemberSet) {
        RegionMXBean regionMXBean = MBeanUtil.getRegionMbeanProxy(member, PARTITIONED_REGION_PATH);
        PartitionAttributesData partitionAttributesData = regionMXBean.listPartitionAttributes();
        assertNotNull(partitionAttributesData);
      }

      ManagementService service = getManagementService();
      DistributedRegionMXBean distributedRegionMXBean = service.getDistributedRegionMXBean(PARTITIONED_REGION_PATH);
      assertEquals(3, distributedRegionMXBean.getMembers().length);
    });
  }

  private void verifyReplicateRegionAfterCreate(final VM memberVM) {
    memberVM.invoke("verifyReplicateRegionAfterCreate", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      String memberId = MBeanJMXAdapter.getMemberNameOrId(cache.getDistributedSystem().getDistributedMember());
      ObjectName memberMBeanName = ObjectName.getInstance("GemFire:type=Member,member=" + memberId);

      MemberNotificationListener listener = new MemberNotificationListener();
      ManagementFactory.getPlatformMBeanServer().addNotificationListener(memberMBeanName, listener, null, null);

      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(REGION_PATH);
      assertNotNull(regionMXBean);

      Region region = cache.getRegion(REGION_PATH);
      RegionAttributes regionAttributes = region.getAttributes();

      RegionAttributesData regionAttributesData = regionMXBean.listRegionAttributes();
      assertRegionAttributes(regionAttributes, regionAttributesData);

      MembershipAttributesData membershipData = regionMXBean.listMembershipAttributes();
      assertNotNull(membershipData);

      EvictionAttributesData evictionData = regionMXBean.listEvictionAttributes();
      assertNotNull(evictionData);
    });
  }

  private void verifyPartitionRegionAfterCreate(final VM memberVM) {
    memberVM.invoke("verifyPartitionRegionAfterCreate", () -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      Region partitionedRegion = cache.getRegion(PARTITIONED_REGION_PATH);

      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionMXBean regionMBean = service.getLocalRegionMBean(PARTITIONED_REGION_PATH);

      assertPartitionData(partitionedRegion.getAttributes(), regionMBean.listPartitionAttributes());
    });
  }

  private void verifyReplicatedRegionAfterClose(final VM memberVM) {
    memberVM.invoke("verifyReplicatedRegionAfterClose", () -> {
      SystemManagementService service = (SystemManagementService) getManagementService();
      RegionMXBean regionMXBean = service.getLocalRegionMBean(REGION_PATH);
      assertNull(regionMXBean);

      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ObjectName regionObjectName = service.getRegionMBeanName(cache.getDistributedSystem().getDistributedMember(), REGION_PATH);
      assertNull(service.getLocalManager().getManagementResourceRepo().getEntryFromLocalMonitoringRegion(regionObjectName));
    });
  }

  private void verifyPartitionRegionAfterClose(final VM memberVM) {
    memberVM.invoke("verifyPartitionRegionAfterClose", () -> {
      ManagementService service = getManagementService();
      RegionMXBean bean = service.getLocalRegionMBean(PARTITIONED_REGION_PATH);
      assertNull(bean);
    });
  }

  /**
   * Invoked in member VMs
   */
  private static void assertPartitionData(final RegionAttributes expectedRegionAttributes, final PartitionAttributesData partitionAttributesData) {
    PartitionAttributes expectedPartitionAttributes = expectedRegionAttributes.getPartitionAttributes();

    assertEquals(expectedPartitionAttributes.getRedundantCopies(), partitionAttributesData.getRedundantCopies());

    assertEquals(expectedPartitionAttributes.getTotalMaxMemory(), partitionAttributesData.getTotalMaxMemory());

    // Total number of buckets for whole region
    assertEquals(expectedPartitionAttributes.getTotalNumBuckets(), partitionAttributesData.getTotalNumBuckets());

    assertEquals(expectedPartitionAttributes.getLocalMaxMemory(), partitionAttributesData.getLocalMaxMemory());

    assertEquals(expectedPartitionAttributes.getColocatedWith(), partitionAttributesData.getColocatedWith());

    String partitionResolver = null;
    if (expectedPartitionAttributes.getPartitionResolver() != null) { // TODO: these conditionals should be deterministic
      partitionResolver = expectedPartitionAttributes.getPartitionResolver().getName();
    }
    assertEquals(partitionResolver, partitionAttributesData.getPartitionResolver());

    assertEquals(expectedPartitionAttributes.getRecoveryDelay(), partitionAttributesData.getRecoveryDelay());

    assertEquals(expectedPartitionAttributes.getStartupRecoveryDelay(), partitionAttributesData.getStartupRecoveryDelay());

    if (expectedPartitionAttributes.getPartitionListeners() != null) {
      for (int i = 0; i < expectedPartitionAttributes.getPartitionListeners().length; i++) {
        assertEquals((expectedPartitionAttributes.getPartitionListeners())[i].getClass().getCanonicalName(), partitionAttributesData.getPartitionListeners()[i]);
      }
    }
  }

  /**
   * Invoked in member VMs
   */
  private static void assertRegionAttributes(final RegionAttributes regionAttributes, final RegionAttributesData regionAttributesData) {
    String compressorClassName = null;
    if (regionAttributes.getCompressor() != null) { // TODO: these conditionals should be deterministic
      compressorClassName = regionAttributes.getCompressor().getClass().getCanonicalName();
    }
    assertEquals(compressorClassName, regionAttributesData.getCompressorClassName());

    String cacheLoaderClassName = null;
    if (regionAttributes.getCacheLoader() != null) {
      cacheLoaderClassName = regionAttributes.getCacheLoader().getClass().getCanonicalName();
    }
    assertEquals(cacheLoaderClassName, regionAttributesData.getCacheLoaderClassName());

    String cacheWriteClassName = null;
    if (regionAttributes.getCacheWriter() != null) {
      cacheWriteClassName = regionAttributes.getCacheWriter().getClass().getCanonicalName();
    }
    assertEquals(cacheWriteClassName, regionAttributesData.getCacheWriterClassName());

    String keyConstraintClassName = null;
    if (regionAttributes.getKeyConstraint() != null) {
      keyConstraintClassName = regionAttributes.getKeyConstraint().getName();
    }
    assertEquals(keyConstraintClassName, regionAttributesData.getKeyConstraintClassName());

    String valueContstaintClassName = null;
    if (regionAttributes.getValueConstraint() != null) {
      valueContstaintClassName = regionAttributes.getValueConstraint().getName();
    }
    assertEquals(valueContstaintClassName, regionAttributesData.getValueConstraintClassName());

    CacheListener[] listeners = regionAttributes.getCacheListeners();
    if (listeners != null) {
      String[] value = regionAttributesData.getCacheListeners();
      for (int i = 0; i < listeners.length; i++) {
        assertEquals(value[i], listeners[i].getClass().getName());
      }
    }

    assertEquals(regionAttributes.getRegionTimeToLive().getTimeout(), regionAttributesData.getRegionTimeToLive());

    assertEquals(regionAttributes.getRegionIdleTimeout().getTimeout(), regionAttributesData.getRegionIdleTimeout());

    assertEquals(regionAttributes.getEntryTimeToLive().getTimeout(), regionAttributesData.getEntryTimeToLive());

    assertEquals(regionAttributes.getEntryIdleTimeout().getTimeout(), regionAttributesData.getEntryIdleTimeout());

    String customEntryTimeToLive = null;
    Object o1 = regionAttributes.getCustomEntryTimeToLive();
    if (o1 != null) {
      customEntryTimeToLive = o1.toString();
    }
    assertEquals(customEntryTimeToLive, regionAttributesData.getCustomEntryTimeToLive());

    String customEntryIdleTimeout = null;
    Object o2 = regionAttributes.getCustomEntryIdleTimeout();
    if (o2 != null) {
      customEntryIdleTimeout = o2.toString();
    }
    assertEquals(customEntryIdleTimeout, regionAttributesData.getCustomEntryIdleTimeout());

    assertEquals(regionAttributes.getIgnoreJTA(), regionAttributesData.isIgnoreJTA());

    assertEquals(regionAttributes.getDataPolicy().toString(), regionAttributesData.getDataPolicy());

    assertEquals(regionAttributes.getScope().toString(), regionAttributesData.getScope());

    assertEquals(regionAttributes.getInitialCapacity(), regionAttributesData.getInitialCapacity());

    assertEquals(regionAttributes.getLoadFactor(), regionAttributesData.getLoadFactor(), 0);

    assertEquals(regionAttributes.isLockGrantor(), regionAttributesData.isLockGrantor());

    assertEquals(regionAttributes.getMulticastEnabled(), regionAttributesData.isMulticastEnabled());

    assertEquals(regionAttributes.getConcurrencyLevel(), regionAttributesData.getConcurrencyLevel());

    assertEquals(regionAttributes.getIndexMaintenanceSynchronous(), regionAttributesData.isIndexMaintenanceSynchronous());

    assertEquals(regionAttributes.getStatisticsEnabled(), regionAttributesData.isStatisticsEnabled());

    assertEquals(regionAttributes.getEnableSubscriptionConflation(), regionAttributesData.isSubscriptionConflationEnabled());

    assertEquals(regionAttributes.getEnableAsyncConflation(), regionAttributesData.isAsyncConflationEnabled());

    assertEquals(regionAttributes.getPoolName(), regionAttributesData.getPoolName());

    assertEquals(regionAttributes.getCloningEnabled(), regionAttributesData.isCloningEnabled());

    assertEquals(regionAttributes.getDiskStoreName(), regionAttributesData.getDiskStoreName());

    String interestPolicy = null;
    if (regionAttributes.getSubscriptionAttributes() != null) {
      interestPolicy = regionAttributes.getSubscriptionAttributes().getInterestPolicy().toString();
    }
    assertEquals(interestPolicy, regionAttributesData.getInterestPolicy());

    assertEquals(regionAttributes.isDiskSynchronous(), regionAttributesData.isDiskSynchronous());
  }

  /**
   * Registered in manager VM
   *
   * User defined notification handler for Region creation handling
   */
  private static class MemberNotificationListener implements NotificationListener {

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      assertNotNull(notification);
      assertTrue(notification.getType().equals(JMXNotificationType.REGION_CREATED) ||
                 notification.getType().equals(JMXNotificationType.REGION_CLOSED));
      // TODO: add better validation
      //LogWriterUtils.getLogWriter().info("<ExpectedString> Member Level Notifications" + notification + "</ExpectedString> ");
    }
  }

  /**
   * Registered in manager VM
   *
   * User defined notification handler for Region creation handling
   */
  private static class DistributedSystemNotificationListener implements NotificationListener {

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      assertNotNull(notification);
      // TODO: add something that will be validated
      //LogWriterUtils.getLogWriter().info("<ExpectedString> Distributed System Notifications" + notification + "</ExpectedString> ");
    }
  }
}
