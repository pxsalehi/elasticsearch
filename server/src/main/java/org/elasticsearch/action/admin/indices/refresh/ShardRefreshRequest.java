/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ShardRefreshRequest extends ReplicationRequest<ShardRefreshRequest> {

    @Nullable
    private final Long primaryTerm;

    @Nullable
    private final Long segmentGeneration;

    public ShardRefreshRequest(ShardId shardId) {
        this(shardId, null, null);
    }

    public ShardRefreshRequest(ShardId shardId, @Nullable Long primaryTerm, @Nullable Long segmentGeneration) {
        super(shardId);
        this.primaryTerm = primaryTerm;
        this.segmentGeneration = segmentGeneration;
    }

    public ShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_8_7_0)) {
            this.primaryTerm = in.readOptionalLong();
            this.segmentGeneration = in.readOptionalLong();
        } else {
            this.primaryTerm = null;
            this.segmentGeneration = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_7_0)) {
            out.writeOptionalLong(primaryTerm);
            out.writeOptionalLong(segmentGeneration);
        }
    }

    @Nullable
    public Long getPrimaryTerm() {
        return primaryTerm;
    }

    @Nullable
    public Long getSegmentGeneration() {
        return segmentGeneration;
    }

    @Override
    public String toString() {
        return "ShardRefreshRequest{" + "shardId=" + shardId + ", segmentGeneration=" + segmentGeneration + ", primaryTerm=" + primaryTerm + '}';
    }
}
