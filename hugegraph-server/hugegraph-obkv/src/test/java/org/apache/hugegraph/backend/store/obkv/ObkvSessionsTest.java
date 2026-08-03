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
import org.apache.hadoop.conf.Configuration;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.alipay.oceanbase.hbase.constants.OHConstants;

public class ObkvSessionsTest {

    private static HugeConfig validConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.param_url", "http://obconfig:8080");
        properties.setProperty("obkv.full_user_name", "root@tenant#cluster");
        properties.setProperty("obkv.password", "secret");
        properties.setProperty("obkv.sys_user_name", "sysroot");
        properties.setProperty("obkv.sys_password", "sys-secret");
        properties.setProperty("obkv.database", "graph");
        return new HugeConfig(properties);
    }

    @Test
    public void testApplyConfiguration() {
        Configuration configuration = ObkvSessions.applyConfiguration(validConfig());
        Assert.assertEquals("http://obconfig:8080",
                            configuration.get(OHConstants.HBASE_OCEANBASE_PARAM_URL));
        Assert.assertEquals("root@tenant#cluster",
                            configuration.get(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME));
        Assert.assertEquals("secret",
                            configuration.get(OHConstants.HBASE_OCEANBASE_PASSWORD));
        Assert.assertEquals("sysroot",
                            configuration.get(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME));
        Assert.assertEquals("sys-secret",
                            configuration.get(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD));
        Assert.assertEquals("graph",
                            configuration.get(OHConstants.HBASE_OCEANBASE_DATABASE));
        Assert.assertEquals(30000, configuration.getInt("rpc.execute.timeout", 0));
    }

    @Test
    public void testValidateAcceptsCompleteConfig() {
        ObkvSessions.validate(validConfig());
    }

    @Test
    public void testValidateRejectsMissingParamUrl() {
        HugeConfig config = validConfig();
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.full_user_name", "root@tenant#cluster");
        properties.setProperty("obkv.sys_user_name", "sysroot");
        properties.setProperty("obkv.database", "graph");
        try {
            ObkvSessions.validate(new HugeConfig(properties));
            Assert.fail("Expected missing param_url failure");
        } catch (IllegalArgumentException e) {
            Assert.assertContains("obkv.param_url", e.getMessage());
        }
    }

    @Test
    public void testValidateRejectsMissingFullUserName() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.param_url", "http://obconfig:8080");
        properties.setProperty("obkv.sys_user_name", "sysroot");
        properties.setProperty("obkv.database", "graph");
        try {
            ObkvSessions.validate(new HugeConfig(properties));
            Assert.fail("Expected missing full_user_name failure");
        } catch (IllegalArgumentException e) {
            Assert.assertContains("obkv.full_user_name", e.getMessage());
        }
    }

    @Test
    public void testValidateRejectsMissingSysUserName() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.param_url", "http://obconfig:8080");
        properties.setProperty("obkv.full_user_name", "root@tenant#cluster");
        properties.setProperty("obkv.database", "graph");
        try {
            ObkvSessions.validate(new HugeConfig(properties));
            Assert.fail("Expected missing sys_user_name failure");
        } catch (IllegalArgumentException e) {
            Assert.assertContains("obkv.sys_user_name", e.getMessage());
        }
    }

    @Test
    public void testValidateRejectsMissingDatabase() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("obkv.param_url", "http://obconfig:8080");
        properties.setProperty("obkv.full_user_name", "root@tenant#cluster");
        properties.setProperty("obkv.sys_user_name", "sysroot");
        try {
            ObkvSessions.validate(new HugeConfig(properties));
            Assert.fail("Expected missing database failure");
        } catch (IllegalArgumentException e) {
            Assert.assertContains("obkv.database", e.getMessage());
        }
    }

    @Test
    public void testConnectionImplementation() {
        ObkvSessions sessions = new ObkvSessions(validConfig(), "graph", "g");
        Assert.assertEquals("com.alipay.oceanbase.hbase.util.OHConnectionImpl",
                            sessions.connectionImplementation());
    }
}
