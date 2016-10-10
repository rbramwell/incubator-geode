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

import static java.util.concurrent.TimeUnit.*;
import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePortHelper.*;
import static org.apache.geode.test.dunit.Host.*;
import static org.apache.geode.test.dunit.NetworkUtils.*;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Properties;

import javax.management.ObjectName;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for managing {@code Locator} with {@link LocatorMXBean}.
 */
@Category(DistributedTest.class)
@SuppressWarnings({ "serial", "unused" })
public class LocatorManagementDUnitTest implements Serializable {

  private static final int MAX_WAIT_MILLIS = 120 * 1000;

  private static final int ZERO = 0;

  @Manager
  private VM managerVM;
  @Member
  private VM[] membersVM;
  private VM locatorVM;

  private String hostName;
  private int port;

  @Rule
  public ManagementTestRule managementTestRule = ManagementTestRule.builder().build();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
//    this.managerVM = managingNode;
//    this.membersVM = getManagedNodeList().toArray(new VM[getManagedNodeList().size()]);
    this.locatorVM = this.membersVM[0];
    this.hostName = getServerHostName(getHost(0));
    this.port = getRandomAvailableTCPPort();
  }

  @After
  public void after() throws Exception {
    stopLocator(this.locatorVM);
  }

  /**
   * When plan is to start Distributed System later so that the system can use
   * this locator
   */
  @Test
  public void testPeerLocation() throws Exception {
    startLocator(this.locatorVM, true, this.port);

    verifyLocalLocatorMXBean(this.locatorVM, this.port, true);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, this.hostName + "[" + this.port + "]");
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "false");
    props.setProperty(JMX_MANAGER_PORT, "0");
    props.setProperty(JMX_MANAGER_HTTP_PORT, "0");

    this.managementTestRule.createManager(this.managerVM, props, false);
    this.managementTestRule.startManager(this.managerVM);

    verifyRemoteLocatorMXBeanProxy(this.managerVM, this.managementTestRule.getDistributedMember(this.locatorVM));
  }

  @Test
  public void testPeerLocationWithPortZero() throws Exception {
    this.port = startLocator(this.locatorVM, true, ZERO);
    //this.locatorVM.invoke(() -> this.managementTestRule.getCache());

    this.locatorVM.invoke(() -> assertHasCache());

    verifyLocalLocatorMXBean(this.locatorVM, this.port, true);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, this.hostName + "[" + this.port + "]");
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "false");
    props.setProperty(JMX_MANAGER_PORT, "0");
    props.setProperty(JMX_MANAGER_HTTP_PORT, "0");

    this.managementTestRule.createManager(this.managerVM, props, false);
    this.managementTestRule.startManager(this.managerVM);

    verifyRemoteLocatorMXBeanProxy(this.managerVM, this.managementTestRule.getDistributedMember(this.locatorVM));
  }

  private void assertHasCache() {
    assertThat(GemFireCacheImpl.getInstance()).isNotNull();
    assertThat(GemFireCacheImpl.getInstance().isClosed()).isFalse();
    assertThat(InternalDistributedSystem.getAnyInstance()).isNotNull();
    assertThat(InternalDistributedSystem.getAnyInstance().isConnected()).isTrue();
  }

  /**
   * Tests a locator which is co-located with already existing cache
   */
  @Test
  public void testColocatedLocator() throws Exception {
    this.managementTestRule.createMembers();
    this.managementTestRule.createManagers();

    startLocator(this.locatorVM, false, this.port);

    verifyLocalLocatorMXBean(this.locatorVM, this.port, false);
  }

  @Test
  public void testColocatedLocatorWithPortZero() throws Exception {
    this.managementTestRule.createMembers();
    this.managementTestRule.createManagers();

    this.port = startLocator(this.locatorVM, false, ZERO);

    verifyLocalLocatorMXBean(this.locatorVM, this.port, false);
  }

  @Test
  public void testListManagers() throws Exception {
    this.managementTestRule.createMembers();
    this.managementTestRule.createManagers();

    startLocator(this.locatorVM, false, this.port);

    verifyListManagers(this.locatorVM);
  }

  @Test
  public void testListManagersWithPortZero() throws Exception {
    this.managementTestRule.createMembers();
    this.managementTestRule.createManagers();

    this.port = startLocator(this.locatorVM, false, ZERO);

    verifyListManagers(this.locatorVM);
  }

  @Test
  public void testWillingManagers() throws Exception {
    startLocator(this.locatorVM, true, this.port);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, this.hostName + "[" + this.port + "]");
    props.setProperty(JMX_MANAGER, "true");

    this.managementTestRule.createMember(this.membersVM[1], props);
    this.managementTestRule.createMember(this.membersVM[2], props);

    verifyListPotentialManagers(this.locatorVM);
  }

  @Test
  public void testWillingManagersWithPortZero() throws Exception {
    this.port = startLocator(this.locatorVM, true, 0);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, this.hostName + "[" + this.port + "]");
    props.setProperty(JMX_MANAGER, "true");

    this.managementTestRule.createMember(this.membersVM[1], props);
    this.managementTestRule.createMember(this.membersVM[2], props);

    verifyListPotentialManagers(this.locatorVM);
  }

  /**
   * Starts a locator with given configuration.
   * If DS is already started it will use the same DS
   */
  private int startLocator(final VM locatorVM, final boolean isPeer, final int port) {
    return locatorVM.invoke("startLocator", () -> {
      assertThat(InternalLocator.hasLocator()).isFalse();

      Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(LOCATORS, "");

      InetAddress bindAddress = InetAddress.getByName(this.hostName);
      File logFile = this.temporaryFolder.newFile(testName.getMethodName() + "-locator-" + port + ".log");
      Locator locator = Locator.startLocatorAndDS(port, logFile, bindAddress, properties, isPeer, true, null);

      assertThat(InternalLocator.hasLocator()).isTrue();

      return locator.getPort();
    });
  }

  private void stopLocator(final VM locatorVM) {
    locatorVM.invoke("stopLocator", () -> {
      assertThat(InternalLocator.hasLocator()).isTrue();
      InternalLocator.getLocator().stop();
    });
  }

  private void verifyLocalLocatorMXBean(final VM locatorVM, final int port, final boolean isPeer) {
    locatorVM.invoke("verifyLocalLocatorMXBean", () -> {
      //ManagementService service = this.managementTestRule.getExistingManagementService();
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ManagementService service = ManagementService.getExistingManagementService(cache);
      assertThat(service).isNotNull();

      LocatorMXBean locatorMXBean = service.getLocalLocatorMXBean();
      assertThat(locatorMXBean).isNotNull();
      assertThat(locatorMXBean.getPort()).isEqualTo(port);

      //        LogWriterUtils.getLogWriter().info("Log of Locator" + bean.viewLog());
      //        LogWriterUtils.getLogWriter().info("BindAddress" + bean.getBindAddress());

      assertThat(locatorMXBean.isPeerLocator()).isEqualTo(isPeer);
    });
  }

  private void verifyRemoteLocatorMXBeanProxy(final VM managerVM, final DistributedMember locatorMember) {
    managerVM.invoke("verifyRemoteLocatorMXBeanProxy", () -> {
      //ManagementService service = this.managementTestRule.getExistingManagementService();
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ManagementService service = ManagementService.getExistingManagementService(cache);
      assertThat(service).isNotNull();

      // LocatorMXBean locatorMXBean = MBeanUtil.getLocatorMbeanProxy(locatorMember); // TODO
      LocatorMXBean locatorMXBean = awaitLockServiceMXBeanProxy(locatorMember);
      assertThat(locatorMXBean).isNotNull();

      //        LogWriterUtils.getLogWriter().info("Log of Locator" + bean.viewLog());
      //        LogWriterUtils.getLogWriter().info("BindAddress" + bean.getBindAddress());
    });
  }

  private void verifyListManagers(final VM locatorVM) {
    locatorVM.invoke("verifyListManagers", () -> {
      //ManagementService service = this.managementTestRule.getExistingManagementService();
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ManagementService service = ManagementService.getExistingManagementService(cache);
      assertThat(service).isNotNull();

      LocatorMXBean locatorMXBean = service.getLocalLocatorMXBean();
      assertThat(locatorMXBean).isNotNull();

      await().until(() -> assertThat(locatorMXBean.listManagers()).hasSize(1));
    });
  }

  private void verifyListPotentialManagers(final VM locatorVM) {
    locatorVM.invoke("verifyListPotentialManagers", () -> {
      //ManagementService service = this.managementTestRule.getExistingManagementService();
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ManagementService service = ManagementService.getExistingManagementService(cache);
      assertThat(service).isNotNull();

      //LocatorMXBean locatorMXBean = service.getLocalLocatorMXBean();
      LocatorMXBean locatorMXBean = awaitLockServiceMXBean();
      assertThat(locatorMXBean).isNotNull();

      await("listPotentialManagers has size 3").until(() -> assertThat(locatorMXBean.listPotentialManagers()).hasSize(3));
    });
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(MAX_WAIT_MILLIS, MILLISECONDS);
  }

  private ConditionFactory await(final String alias) {
    return Awaitility.await(alias).atMost(MAX_WAIT_MILLIS, MILLISECONDS);
  }

  /**
   * Await and return a LocatorMXBean proxy for a specific member.
   */
  private LocatorMXBean awaitLockServiceMXBeanProxy(final DistributedMember member) {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();
    ObjectName locatorMBeanName = service.getLocatorMBeanName(member);

    await().until(() -> assertThat(service.getMBeanProxy(locatorMBeanName, LocatorMXBean.class)).isNotNull());

    return service.getMBeanProxy(locatorMBeanName, LocatorMXBean.class);
  }

  /**
   * Await creation of local LocatorMXBean.
   */
  private LocatorMXBean awaitLockServiceMXBean() {
    SystemManagementService service = this.managementTestRule.getSystemManagementService();

    await().until(() -> assertThat(service.getLocalLocatorMXBean()).isNotNull());

    return service.getLocalLocatorMXBean();
  }

  public static String getServerHostName(Host host) {
    return System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "server-bind-address") != null ?
      System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "server-bind-address") : host.getHostName();
  }
}
