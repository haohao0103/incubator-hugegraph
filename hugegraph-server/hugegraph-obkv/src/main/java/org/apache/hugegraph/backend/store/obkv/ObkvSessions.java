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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hugegraph.backend.store.hbase.HbaseSessions;
import org.apache.hugegraph.config.HugeConfig;

import com.alipay.oceanbase.hbase.constants.OHConstants;

/**
 * HBase 2.x compatible session configuration for OBKV-HBase 2.5.0.
 * The actual Connection implementation is loaded by HBase ConnectionFactory
 * from the configured OHConnectionImpl class.
 */
public class ObkvSessions extends HbaseSessions {

    private static final String OH_CONNECTION_IMPL =
            "com.alipay.oceanbase.hbase.util.OHConnectionImpl";

    public ObkvSessions(HugeConfig config, String namespace, String store) {
        super(config, namespace, store);
    }

    @Override
    protected String connectionImplementation() {
        return OH_CONNECTION_IMPL;
    }

    @Override
    protected void configureConnection(Configuration configuration,
                                      HugeConfig config) {
        Configuration obkv = applyConfiguration(config);
        for (Map.Entry<String, String> entry : obkv) {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    public static Configuration applyConfiguration(HugeConfig config) {
        Configuration hConfig = new Configuration(false);
        hConfig.set(OHConstants.HBASE_OCEANBASE_PARAM_URL,
                    config.get(ObkvOptions.PARAM_URL));
        hConfig.set(OHConstants.HBASE_OCEANBASE_FULL_USER_NAME,
                    config.get(ObkvOptions.FULL_USER_NAME));
        hConfig.set(OHConstants.HBASE_OCEANBASE_PASSWORD,
                    config.get(ObkvOptions.PASSWORD));
        hConfig.set(OHConstants.HBASE_OCEANBASE_SYS_USER_NAME,
                    config.get(ObkvOptions.SYS_USER_NAME));
        hConfig.set(OHConstants.HBASE_OCEANBASE_SYS_PASSWORD,
                    config.get(ObkvOptions.SYS_PASSWORD));
        hConfig.set(OHConstants.HBASE_OCEANBASE_DATABASE,
                    config.get(ObkvOptions.DATABASE));
        hConfig.setInt("rpc.execute.timeout",
                       config.get(ObkvOptions.OPERATION_TIMEOUT));
        return hConfig;
    }

    public static void validate(HugeConfig config) {
        require(ObkvOptions.PARAM_URL, config.get(ObkvOptions.PARAM_URL));
        require(ObkvOptions.FULL_USER_NAME, config.get(ObkvOptions.FULL_USER_NAME));
        require(ObkvOptions.SYS_USER_NAME, config.get(ObkvOptions.SYS_USER_NAME));
        require(ObkvOptions.DATABASE, config.get(ObkvOptions.DATABASE));
    }

    private static void require(Object option, String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required OBKV option: " + option);
        }
    }
}
