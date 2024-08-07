/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.core;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;


public class BaseCoreTest {

    @BeforeClass
    public static void setup() throws Exception {

    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.printf("Failed to start ....,%s%n", e.getMessage());
        }
    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}
