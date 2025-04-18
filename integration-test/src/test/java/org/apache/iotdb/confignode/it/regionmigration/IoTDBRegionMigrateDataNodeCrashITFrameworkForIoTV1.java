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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.it.regionmigration;

import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;

import org.junit.Before;

public class IoTDBRegionMigrateDataNodeCrashITFrameworkForIoTV1
    extends IoTDBRegionOperationReliabilityITFramework {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
  }

  @SafeVarargs
  public final <T extends Enum<T>> void success(T... dataNodeKillPoints) throws Exception {
    successTest(1, 1, 1, 2, noKillPoints(), buildSet(dataNodeKillPoints), KillNode.ALL_NODES);
  }
}
