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

import static org.apache.hugegraph.config.OptionChecker.positiveInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public final class ObkvOptions extends OptionHolder {

    public static final ConfigOption<String> PARAM_URL = new ConfigOption<>(
            "obkv.param_url", "OceanBase ConfigServer URL", null, "");
    public static final ConfigOption<String> FULL_USER_NAME = new ConfigOption<>(
            "obkv.full_user_name", "OceanBase full user name", "");
    public static final ConfigOption<String> PASSWORD = new ConfigOption<>(
            "obkv.password", "OceanBase user password", null, "");
    public static final ConfigOption<String> SYS_USER_NAME = new ConfigOption<>(
            "obkv.sys_user_name", "OceanBase sys user name", "");
    public static final ConfigOption<String> SYS_PASSWORD = new ConfigOption<>(
            "obkv.sys_password", "OceanBase sys user password", null, "");
    public static final ConfigOption<String> DATABASE = new ConfigOption<>(
            "obkv.database", "OceanBase database", "");
    public static final ConfigOption<Integer> OPERATION_TIMEOUT = new ConfigOption<>(
            "obkv.operation_timeout", "OBKV operation timeout in milliseconds",
            positiveInt(), 30000);
    public static final ConfigOption<Integer> RETRY_TIMES = new ConfigOption<>(
            "obkv.retry_times", "OBKV retry count", positiveInt(), 3);
    public static final ConfigOption<Boolean> ENABLE_PARTITION = new ConfigOption<>(
            "obkv.enable_partition", "Enable HugeGraph row-key partitions", null, true);
    public static final ConfigOption<Integer> VERTEX_PARTITIONS = new ConfigOption<>(
            "obkv.vertex_partitions", "Vertex logical partition count", positiveInt(), 10);
    public static final ConfigOption<Integer> EDGE_PARTITIONS = new ConfigOption<>(
            "obkv.edge_partitions", "Edge logical partition count", positiveInt(), 30);

    private static volatile ObkvOptions instance;

    private ObkvOptions() {
        super();
    }

    public static synchronized ObkvOptions instance() {
        if (instance == null) {
            instance = new ObkvOptions();
            instance.registerOptions();
        }
        return instance;
    }
}
