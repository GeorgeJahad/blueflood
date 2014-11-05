/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.inputs.handlers;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.cache.ConfigTtlProvider;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.http.HttpRequestHandler;
import com.rackspacecloud.blueflood.http.HttpResponder;
import com.rackspacecloud.blueflood.inputs.formats.JSONMetricsContainer;
import com.rackspacecloud.blueflood.inputs.processors.BatchWriter;
import com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter;
import com.rackspacecloud.blueflood.inputs.processors.RollupTypeCacher;
import com.rackspacecloud.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.rackspacecloud.blueflood.io.Constants;
import com.rackspacecloud.blueflood.io.IMetricsWriter;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.MetricsCollection;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.rackspacecloud.blueflood.service.*;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public class HttpMetricsIngestionHandler implements HttpRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(HttpMetricsIngestionHandler.class);

    protected final ObjectMapper mapper;
    protected final TypeFactory typeFactory;
    private final TypeAndUnitProcessor typeAndUnitProcessor;
    private final RollupTypeCacher rollupTypeCacher;
    private final DiscoveryWriter discoveryWriter;
    private final BatchWriter batchWriter;
    private ScheduleContext context;
    private IMetricsWriter writer;
    private final TimeValue timeout;
    private IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());
    private int HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS = Configuration.getInstance().getIntegerProperty(HttpConfig.HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS);
    private final Counter bufferedMetrics = Metrics.counter(HttpMetricsIngestionHandler.class, "Buffered Metrics");
    private static int BATCH_SIZE = Configuration.getInstance().getIntegerProperty(CoreConfig.METRIC_BATCH_SIZE);
    private static int WRITE_THREADS = Configuration.getInstance().getIntegerProperty(CoreConfig.METRICS_BATCH_WRITER_THREADS); // metrics will be batched into this many partitions.
    private static TimeValue DEFAULT_TIMEOUT = new TimeValue(5, TimeUnit.SECONDS);

    // Metrics
    private static final Timer jsonTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion json processing timer");
    private static final Timer persistingTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion persisting timer");
    private static final Timer sendResponseTimer = Metrics.timer(HttpMetricsIngestionHandler.class, "HTTP Ingestion response sending timer");


    public HttpMetricsIngestionHandler(ScheduleContext context, IMetricsWriter writer) {
        this.mapper = new ObjectMapper();
        this.typeFactory = TypeFactory.defaultInstance();
        this.timeout = DEFAULT_TIMEOUT; //TODO: make configurable
        this.context = context;
        this.writer = writer;

        typeAndUnitProcessor = 
            new TypeAndUnitProcessor(new ThreadPoolBuilder()
                .withName("Metric type and unit processing")
                .withCorePoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                .withMaxPoolSize(HTTP_MAX_TYPE_UNIT_PROCESSOR_THREADS)
                .build(),
                metricMetadataAnalyzer);
        typeAndUnitProcessor.withLogger(log);

        // RollupRunnable keeps a static one of these. It would be nice if we could register it and share.
        MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
                new TimeValue(48, TimeUnit.HOURS),
                Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));
        rollupTypeCacher = new RollupTypeCacher(
                new ThreadPoolBuilder().withName("Rollup type persistence").build(),
                rollupTypeCache);
        rollupTypeCacher.withLogger(log);

        discoveryWriter =
        new DiscoveryWriter(new ThreadPoolBuilder()
            .withName("Metric Discovery Writing")
            .withCorePoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.DISCOVERY_WRITER_MIN_THREADS))
            .withMaxPoolSize(Configuration.getInstance().getIntegerProperty(CoreConfig.DISCOVERY_WRITER_MAX_THREADS))
            .withUnboundedQueue()
            .build());
        discoveryWriter.withLogger(log);

        batchWriter = new BatchWriter(
                new ThreadPoolBuilder()
                        .withName("Metric Batch Writing")
                        .withCorePoolSize(WRITE_THREADS)
                        .withMaxPoolSize(WRITE_THREADS)
                        .withUnboundedQueue()
                        .build(),
                writer,
                timeout,
                bufferedMetrics,
                context
        );
        batchWriter.withLogger(log);

    }

    protected JSONMetricsContainer createContainer(String body, String tenantId) throws JsonParseException, JsonMappingException, IOException {
        List<JSONMetricsContainer.JSONMetric> jsonMetrics =
                mapper.readValue(
                        body,
                        typeFactory.constructCollectionType(List.class,
                                JSONMetricsContainer.JSONMetric.class)
                );
        return new JSONMetricsContainer(tenantId, jsonMetrics);
    }

    @Override
    public void handle(ChannelHandlerContext ctx, HttpRequest request) {
        final String tenantId = request.getHeader("tenantId");
        JSONMetricsContainer jsonMetricsContainer = null;
        final Timer.Context jsonTimerContext = jsonTimer.time();

        final String body = request.getContent().toString(Constants.DEFAULT_CHARSET);
        try {
            jsonMetricsContainer = createContainer(body, tenantId);
            if (!jsonMetricsContainer.isValid()) {
                throw new IOException("Invalid JSONMetricsContainer");
            }
        } catch (JsonParseException e) {
            log.warn("Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (JsonMappingException e) {
            log.warn("Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (IOException e) {
            log.warn("IO Exception parsing content", e);
            sendResponse(ctx, request, "Cannot parse content", HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (Exception e) {
            log.warn("Other exception while trying to parse content", e);
            sendResponse(ctx, request, "Failed parsing content", HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }

        if (jsonMetricsContainer == null) {
            log.warn(ctx.getChannel().getRemoteAddress() + " No valid metrics");
            sendResponse(ctx, request, "No valid metrics", HttpResponseStatus.BAD_REQUEST);
            return;
        }

        List<Metric> containerMetrics;
        try {
            containerMetrics = jsonMetricsContainer.toMetrics();
            forceTTLsIfConfigured(containerMetrics);
        } catch (InvalidDataException ex) {
            // todo: we should measure these. if they spike, we track down the bad client.
            // this is strictly a client problem. Someting wasn't right (data out of range, etc.)
            log.warn(ctx.getChannel().getRemoteAddress() + " " + ex.getMessage());
            sendResponse(ctx, request, "Invalid data " + ex.getMessage(), HttpResponseStatus.BAD_REQUEST);
            return;
        } catch (Exception e) {
            // todo: when you see these in logs, go and fix them (throw InvalidDataExceptions) so they can be reduced
            // to single-line log statements.
            log.warn("Exception converting JSON container to metric objects", e);
            // This could happen if clients send BigIntegers as metric values. BF doesn't handle them. So let's send a
            // BAD REQUEST message until we start handling BigIntegers.
            sendResponse(ctx, request, "Error converting JSON payload to metric objects",
                    HttpResponseStatus.BAD_REQUEST);
            return;
        } finally {
            jsonTimerContext.stop();
        }

        if (containerMetrics == null || containerMetrics.isEmpty()) {
            log.warn(ctx.getChannel().getRemoteAddress() + " No valid metrics");
            sendResponse(ctx, request, "No valid metrics", HttpResponseStatus.BAD_REQUEST);
        }

        final MetricsCollection collection = new MetricsCollection();
        collection.add(new ArrayList<IMetric>(containerMetrics));
        final Timer.Context persistingTimerContext = persistingTimer.time();
        try {
            typeAndUnitProcessor.apply(collection);
            rollupTypeCacher.apply(collection);
            List<List<IMetric>> batches  = collection.splitMetricsIntoBatches(BATCH_SIZE);
            discoveryWriter.apply(batches);
            ListenableFuture<List<Boolean>> futures = batchWriter.apply(batches);
	    log.error("gbjfixingHandler3\n");
            List<Boolean> persisteds = futures.get(timeout.getValue(), timeout.getUnit());
            for (Boolean persisted : persisteds) {
                if (!persisted) {
                    sendResponse(ctx, request, null, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    return;
                }
            }
            sendResponse(ctx, request, null, HttpResponseStatus.OK);
        } catch (TimeoutException e) {
            sendResponse(ctx, request, "Timed out persisting metrics", HttpResponseStatus.ACCEPTED);
        } catch (Exception e) {
            log.error("Exception persisting metrics", e);
            sendResponse(ctx, request, "Error persisting metrics", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            persistingTimerContext.stop();
        }
    }

    private void forceTTLsIfConfigured(List<Metric> containerMetrics) {
        ConfigTtlProvider configTtlProvider = ConfigTtlProvider.getInstance();

        if(configTtlProvider.areTTLsForced()) {
            for(Metric m : containerMetrics) {
                m.setTtl(configTtlProvider.getConfigTTLForIngestion());
            }
        }
    }

    public static void sendResponse(ChannelHandlerContext channel, HttpRequest request, String messageBody, HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        final Timer.Context sendResponseTimerContext = sendResponseTimer.time();

        try {
            if (messageBody != null && !messageBody.isEmpty()) {
                response.setContent(ChannelBuffers.copiedBuffer(messageBody, Constants.DEFAULT_CHARSET));
            }
            HttpResponder.respond(channel, request, response);
        } finally {
            sendResponseTimerContext.stop();
        }
    }
}
