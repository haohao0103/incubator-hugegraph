///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.hugegraph.api.filter;
//
//import static org.apache.hugegraph.api.filter.PathFilter.REQUEST_TIME;
//import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_FAILED_COUNTER;
//import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_RESPONSE_TIME_HISTOGRAM;
//import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_SUCCESS_COUNTER;
//import static org.apache.hugegraph.metrics.MetricsUtil.METRICS_PATH_TOTAL_COUNTER;
//
//import java.io.IOException;
//import java.net.URI;
//
//import org.apache.hugegraph.auth.HugeAuthenticator;
//import org.apache.hugegraph.config.HugeConfig;
//import org.apache.hugegraph.config.ServerOptions;
//import org.apache.hugegraph.core.GraphManager;
//import org.apache.hugegraph.metrics.MetricsUtil;
//import org.apache.hugegraph.util.Log;
//import org.slf4j.Logger;
//
//import jakarta.inject.Singleton;
//import jakarta.ws.rs.HttpMethod;
//import jakarta.ws.rs.container.ContainerRequestContext;
//import jakarta.ws.rs.container.ContainerResponseContext;
//import jakarta.ws.rs.container.ContainerResponseFilter;
//import jakarta.ws.rs.core.Context;
//import jakarta.ws.rs.ext.Provider;
//
//// TODO: should add test for this class
//@Provider
//@Singleton
//public class AccessLogFilter implements ContainerResponseFilter {
//
//    private static final Logger LOG = Log.logger(AccessLogFilter.class);
//
//    private static final String DELIMITER = "/";
//    private static final String GRAPHS = "graphs";
//    private static final String GREMLIN = "gremlin";
//    private static final String CYPHER = "cypher";
//
//    @Context
//    private jakarta.inject.Provider<HugeConfig> configProvider;
//
//    @Context
//    private jakarta.inject.Provider<GraphManager> managerProvider;
//
//    public static boolean needRecordLog(ContainerRequestContext context) {
//        // TODO: add test for 'path' result ('/gremlin' or 'gremlin')
//        String path = context.getUriInfo().getPath();
//
//        // GraphsAPI/CypherAPI/Job GremlinAPI
//        if (path.startsWith(GRAPHS)) {
//            if (HttpMethod.GET.equals(context.getMethod()) || path.endsWith(CYPHER)) {
//                return true;
//            }
//        }
//        // Direct GremlinAPI
//        return path.endsWith(GREMLIN);
//    }
//
//    private String join(String path1, String path2) {
//        return String.join(DELIMITER, path1, path2);
//    }
//
//    /**
//     * Use filter to log request info
//     *
//     * @param requestContext  requestContext
//     * @param responseContext responseContext
//     */
//    @Override
//    public void filter(ContainerRequestContext requestContext,
//                       ContainerResponseContext responseContext) throws IOException {
//        // Grab corresponding request / response info from context;
//        URI uri = requestContext.getUriInfo().getRequestUri();
//        String path = uri.getRawPath();
//        String method = requestContext.getMethod();
//        String metricsName = join(path, method);
//
//        MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_TOTAL_COUNTER)).inc();
//        if (statusOk(responseContext.getStatus())) {
//            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_SUCCESS_COUNTER)).inc();
//        } else {
//            MetricsUtil.registerCounter(join(metricsName, METRICS_PATH_FAILED_COUNTER)).inc();
//        }
//
//        Object requestTime = requestContext.getProperty(REQUEST_TIME);
//        if (requestTime != null) {
//            long now = System.currentTimeMillis();
//            long start = (Long) requestTime;
//            long executeTime = now - start;
//
//            MetricsUtil.registerHistogram(join(metricsName, METRICS_PATH_RESPONSE_TIME_HISTOGRAM))
//                       .update(executeTime);
//
//            HugeConfig config = configProvider.get();
//            long timeThreshold = config.get(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD);
//            // Record slow query if meet needs, watch out the perf
//            if (timeThreshold > 0 && executeTime > timeThreshold &&
//                needRecordLog(requestContext)) {
//                // TODO: set RequestBody null, handle it later & should record "client IP"
//                LOG.info("[Slow Query] execTime={}ms, body={}, method={}, path={}, query={}",
//                         executeTime, null, method, path, uri.getQuery());
//            }
//        }
//
//        // Unset the context in "HugeAuthenticator", need distinguish Graph/Auth server lifecycle
//        GraphManager manager = managerProvider.get();
//        // TODO: transfer Authorizer if we need after.
//        if (manager.requireAuthentication()) {
//            manager.unauthorize(requestContext.getSecurityContext());
//        }
//    }
//
//    private boolean statusOk(int status) {
//        return status >= 200 && status < 300;
//    }
//}


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

package org.apache.hugegraph.api.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Singleton;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.Provider;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.hugegraph.api.gremlin.GremlinRequestContext;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.metrics.MetricsUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.hugegraph.api.filter.PathFilter.REQUEST_TIME;
import static org.apache.hugegraph.metrics.MetricsUtil.*;

// TODO: should add test for this class
@Provider
@Singleton
public class AccessLogFilter implements ContainerResponseFilter {

    private static final Logger LOG = Log.logger(AccessLogFilter.class);
    // 定义多个不同用途的 logger
    // 普通API请求
    private static final Logger API_LOG = Log.logger("hugegraph.access.api");
    // Gremlin请求
    private static final Logger GREMLIN_LOG = Log.logger("hugegraph.access.gremlin");




    private static final String DELIMITER = "/";
    private static final String GRAPHS = "graphs";
    private static final String GREMLIN = "gremlin";
    private static final String CYPHER = "cypher";
    private static final ObjectMapper MAPPER = new ObjectMapper();


    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;


    public static boolean needRecordLog(ContainerRequestContext context) {
        // TODO: add test for 'path' result ('/gremlin' or 'gremlin')
        String path = context.getUriInfo().getPath();

        // GraphsAPI/CypherAPI/Job GremlinAPI
        if (path.startsWith(GRAPHS) ) {
            if (HttpMethod.GET.equals(context.getMethod()) || path.endsWith(CYPHER)) {
                return true;
            }
        }
        // Direct GremlinAPI
        return path.endsWith(GREMLIN);
    }

    public static boolean needRecordGremlinLog(ContainerRequestContext context) {
        // TODO: add test for 'path' result ('/gremlin' or 'gremlin')
        String path = context.getUriInfo().getPath();
        if (path.startsWith(GREMLIN) || path.endsWith(GREMLIN) ) {
            return true;
        }
        return false;
    }

    private String join(String path1, String path2) {
        return String.join(DELIMITER, path1, path2);
    }

    private String normalizePath(ContainerRequestContext requestContext) {
        // Replace variable parts of the path with placeholders
        //TODO: 判断此方法参数是在路径上的
        /**
         * 核心逻辑
         * 1. 判断此路径是否含有参数
         * 2. 如果是，则归一化处理
         * 3. 如果不是,则不处理，直接返回路径
         *
         * 根因定位
         * 1. 如果返回码正常 则归一化正常
         * 2. todo: 如果返回码异常？ 怎么做：
         */

        String requestPath = requestContext.getUriInfo().getPath();
        // 获取路径参数的值
        MultivaluedMap<String, String> pathParameters = requestContext.getUriInfo().getPathParameters();

        String newPath = requestPath;
        for (Map.Entry<String, java.util.List<String>> entry : pathParameters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().get(0); // 获取第一个值
            if(key.equals("graph")){
                newPath = newPath.replace(key, value);
            }
            newPath = newPath.replace(value, key);
        }

        LOG.debug("Original Path: " + requestPath + " New Path: " + newPath);

        return newPath;
    }


    private static final String REQUEST_BODY_FLAG = "request.has.body";
    /**
     * Use filter to log request info
     *
     * @param requestContext  requestContext
     * @param responseContext responseContext
     */
    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) throws IOException {
        try {
            URI uri = requestContext.getUriInfo().getRequestUri();
            String method = requestContext.getMethod();
            String path = normalizePath(requestContext);
            String metricsName = join(path, method);
            int status = responseContext.getStatus();
            Object params = GremlinRequestContext.getRequestParams();



            Object requestTime = requestContext.getProperty(REQUEST_TIME);
            if (requestTime != null) {
                long executeTime = System.currentTimeMillis() - (Long) requestTime;


                // 获取配置
                HugeConfig config = configProvider.get();

                // 获取响应体
                String responseBody = getResponseBody(responseContext, config);

                // 获取查询参数
                String query = getQueryParameters(requestContext, uri);

                // 记录API日志
                if (needRecordLog(requestContext)) {
                    logApiRequest(method, uri, query,  responseContext.getStatus(),
                            executeTime, responseBody);
                }
                // 记录gremlin 日志
                if(needRecordGremlinLog(requestContext)){
                    logGremlinRequest(method, uri, query, handleGremlinQuery(params), responseContext.getStatus(),
                            executeTime, responseBody);
                }
                // 记录慢查询日志
                handleSlowQueryLog(config, executeTime, method, path, uri, requestContext);
            }


        } finally {
            GremlinRequestContext.clear();
        }
    }

    private String handleGremlinQuery( Object params){
        String gremlinQuery = null;
        if (params != null) {
            if (params instanceof Entity) {
                // 处理 POST 请求参数
                Entity<?> body = (Entity<?>) params;
                gremlinQuery = body.getEntity().toString();
            } else if (params instanceof MultivaluedMap) {
                // 处理 GET 请求参数
                @SuppressWarnings("unchecked")
                MultivaluedMap<String, String> queryParams = (MultivaluedMap<String, String>) params;
                gremlinQuery = queryParams.getFirst("gremlin");
            }
        }
        return gremlinQuery;
    }


//    private String getResponseBody(ContainerResponseContext responseContext, HugeConfig config) throws UnsupportedEncodingException {
//
//            Object responseEntity = responseContext.getEntity();
//            if (responseEntity != null) {
//                return truncateIfNeeded(responseEntity.toString(),
//                        config.get(ServerOptions.RESPONSE_LENGTH_LOG_THRESHOLD));
//            }
//            return null;
//
//
//    }



    private String getResponseBody(ContainerResponseContext responseContext, HugeConfig config) throws IOException {
        Object responseEntity = responseContext.getEntity();
        if (responseEntity == null) {
            return null;
        }

        // 处理字符串类型的响应
        if (responseEntity instanceof String) {
            return truncateIfNeeded((String) responseEntity,
                    config.get(ServerOptions.RESPONSE_LENGTH_LOG_THRESHOLD));
        }

        // 处理字节数组类型的响应
        if (responseEntity instanceof byte[]) {
            byte[] bytes = (byte[]) responseEntity;
            return truncateIfNeeded(new String(bytes, StandardCharsets.UTF_8),
                    config.get(ServerOptions.RESPONSE_LENGTH_LOG_THRESHOLD));
        }

        // 处理流类型的响应
        if (responseEntity instanceof InputStream) {
            InputStream inputStream = (InputStream) responseEntity;
            ByteArrayOutputStream copyStream = new ByteArrayOutputStream();

            // 使用 TeeInputStream 复制流数据
            TeeInputStream teeInputStream = new TeeInputStream(inputStream, copyStream);

            // 读取流中的数据
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = teeInputStream.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }

            // 将读取的数据转换为字符串
            String responseBody = truncateIfNeeded(result.toString(StandardCharsets.UTF_8.name()),
                    config.get(ServerOptions.RESPONSE_LENGTH_LOG_THRESHOLD));

            // 将复制的内容重新设置为响应实体
            responseContext.setEntity(new ByteArrayInputStream(copyStream.toByteArray()));

            return responseBody;
        }

        // 处理自定义对象类型的响应
        try {
            // 使用 ObjectMapper 将对象序列化为 JSON 字符串
            return MAPPER.writeValueAsString(responseEntity);
        } catch (Exception e) {
            LOG.warn("Failed to serialize response entity: {}", responseEntity, e);
            return responseEntity.toString(); // 如果序列化失败，返回默认的 toString() 结果
        }
    }

    private String getQueryParameters(ContainerRequestContext requestContext, URI uri) {
        String query = uri.getQuery();
        if (query == null) {
            MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
            query = queryParams.isEmpty() ? null : queryParams.toString();
        }
        return query;
    }


    private void logApiRequest(String method, URI uri, String query,
                               int status, long executeTime, String responseBody) {
        API_LOG.info("API Request - method={}, path={}, query={},  status={}, cost={}ms, response={}",
                method, uri.getRawPath(), query,  status, executeTime, responseBody);
    }

    private void logGremlinRequest(String method, URI uri, String query,String requestBody,
                                   int status, long executeTime, String responseBody) {
        GREMLIN_LOG.info("Gremlin Request - method={}, path={}, query={}, requestBody={}, status={}, cost={}ms, response={}",
                method, uri.getRawPath(), query, requestBody, status, executeTime, responseBody);
    }

    private void handleSlowQueryLog(HugeConfig config, long executeTime, String method,
                                    String path, URI uri, ContainerRequestContext requestContext) {
        long timeThreshold = config.get(ServerOptions.SLOW_QUERY_LOG_TIME_THRESHOLD);
        if (timeThreshold > 0 && executeTime > timeThreshold && needRecordLog(requestContext)) {
            LOG.info("[Slow Query] execTime={}ms, body={}, method={}, path={}, query={}",
                    executeTime, null, method, path, uri.getQuery());
        }
    }



    private boolean statusOk(int status) {
        return status >= 200 && status < 300;
    }

    private String truncateIfNeeded(String content, int bodyLengthThreshold){
        if (content == null) {
            return null;
        }
        if (content.length() <= bodyLengthThreshold) {
            return content;
        }
        return content.substring(0, bodyLengthThreshold) + "... (truncated)";
    }

    private static class LoggingOutputStream extends OutputStream {
        private final OutputStream original;
        private final ByteArrayOutputStream copy;

        public LoggingOutputStream(OutputStream original, ByteArrayOutputStream copy) {
            this.original = original;
            this.copy = copy;
        }

        @Override
        public void write(int b) throws IOException {
            original.write(b);
            copy.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            original.write(b, off, len);
            copy.write(b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            original.write(b);
            copy.write(b);
        }
    }
}



