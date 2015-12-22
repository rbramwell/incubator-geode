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
package com.gemstone.gemfire.test.dunit.tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.gemstone.gemfire.distributed.DistributedMemberDUnitTest;
import com.gemstone.gemfire.distributed.HostedLocatorsDUnitTest;
import com.gemstone.gemfire.internal.offheap.OutOfOffHeapMemoryDUnitTest;
import com.gemstone.gemfire.test.examples.CatchExceptionExampleDUnitTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
//  BasicDUnitTest.class,
//  DistributedTestNameDUnitTest.class,
  DistributedTestNameWithRuleDUnitTest.class,
  SerializableTemporaryFolderDUnitTest.class,
  SerializableTestNameDUnitTest.class,
  SerializableTestWatcherDUnitTest.class,
//  VMDUnitTest.class,
//  VMMoreDUnitTest.class,
  
  CatchExceptionExampleDUnitTest.class,
  DistributedMemberDUnitTest.class,
//  HostedLocatorsDUnitTest.class,
//  OutOfOffHeapMemoryDUnitTest.class,
})
public class DistributedTestRuleTestSuite {
}
