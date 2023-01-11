/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BasicReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardRefreshAction extends TransportReplicationAction<
    BasicReplicationRequest,
    ShardRefreshRequest,
    ReplicationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportShardRefreshAction.class);

    public static final String NAME = RefreshAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);
    public static final String SOURCE_API = "api";

    private final Settings settings;

    @Inject
    public TransportShardRefreshAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BasicReplicationRequest::new,
            ShardRefreshRequest::new,
            ThreadPool.Names.REFRESH
        );
        this.settings = settings;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        BasicReplicationRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardRefreshRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            final long term = primary.getOperationPrimaryTerm();
            var res = primary.refresh(SOURCE_API);
            logger.trace("{} refresh request executed on primary", primary.shardId());
            var shardRefreshRequest = new ShardRefreshRequest(primary.shardId(), primary.getOperationPrimaryTerm(), res.getSegmentGeneration());
            return new PrimaryResult<>(shardRefreshRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardRefreshRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        if (DiscoveryNode.isStateless(settings)) {
            // wait for the segment number mentioned in the request, and then reply back.
            long primaryTerm = request.getPrimaryTerm(); // TODO: do we need to explicitly check the primaryTerm here?
            long segmentGeneration = request.getSegmentGeneration();
            replica.waitForSegmentGeneration(segmentGeneration, listener.map(gen -> new ReplicaResult()));
        } else {
            ActionListener.completeWith(listener, () -> {
                replica.refresh(SOURCE_API);
                logger.trace("{} refresh request executed on replica", replica.shardId());
                return new ReplicaResult();
            });
        }
    }
}
