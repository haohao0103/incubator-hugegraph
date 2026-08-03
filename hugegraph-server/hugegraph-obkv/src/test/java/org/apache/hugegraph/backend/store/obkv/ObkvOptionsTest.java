/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hugegraph.backend.store.obkv;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.testutil.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObkvOptionsTest {

    @BeforeClass
    public static void register() {
        OptionSpace.register("obkv", ObkvOptions.class.getName());
    }

    @Test
    public void testDefaults() {
        HugeConfig config = new HugeConfig(new PropertiesConfiguration());
        Assert.assertEquals(30000, config.get(ObkvOptions.OPERATION_TIMEOUT));
        Assert.assertEquals(3, config.get(ObkvOptions.RETRY_TIMES));
        Assert.assertTrue(config.get(ObkvOptions.ENABLE_PARTITION));
        Assert.assertEquals(10, config.get(ObkvOptions.VERTEX_PARTITIONS));
        Assert.assertEquals(30, config.get(ObkvOptions.EDGE_PARTITIONS));
    }

    @Test
    public void testValues() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.param_url", "http://obconfig:8080");
        properties.setProperty("obkv.full_user_name", "root@tenant#cluster");
        properties.setProperty("obkv.password", "secret");
        properties.setProperty("obkv.sys_user_name", "sysroot");
        properties.setProperty("obkv.sys_password", "sys-secret");
        properties.setProperty("obkv.database", "graph");
        properties.setProperty("obkv.operation_timeout", "1234");
        properties.setProperty("obkv.retry_times", "5");
        properties.setProperty("obkv.enable_partition", "false");

        HugeConfig config = new HugeConfig(properties);
        Assert.assertEquals("http://obconfig:8080",
                            config.get(ObkvOptions.PARAM_URL));
        Assert.assertEquals("root@tenant#cluster",
                            config.get(ObkvOptions.FULL_USER_NAME));
        Assert.assertEquals("secret", config.get(ObkvOptions.PASSWORD));
        Assert.assertEquals("sysroot", config.get(ObkvOptions.SYS_USER_NAME));
        Assert.assertEquals("sys-secret", config.get(ObkvOptions.SYS_PASSWORD));
        Assert.assertEquals("graph", config.get(ObkvOptions.DATABASE));
        Assert.assertEquals(1234, config.get(ObkvOptions.OPERATION_TIMEOUT));
        Assert.assertEquals(5, config.get(ObkvOptions.RETRY_TIMES));
        Assert.assertFalse(config.get(ObkvOptions.ENABLE_PARTITION));
    }
}
