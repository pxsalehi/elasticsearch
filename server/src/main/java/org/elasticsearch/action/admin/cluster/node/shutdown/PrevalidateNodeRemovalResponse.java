/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class PrevalidateNodeRemovalResponse extends ActionResponse implements ToXContentObject {
    private final NodesRemovalPrevalidation prevalidation;

    public PrevalidateNodeRemovalResponse(final NodesRemovalPrevalidation prevalidation) {
        this.prevalidation = prevalidation;
    }

    public NodesRemovalPrevalidation getPrevalidation() {
        return prevalidation;
    }

    public PrevalidateNodeRemovalResponse(StreamInput in) throws IOException {
        super(in);
        prevalidation = new NodesRemovalPrevalidation(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        prevalidation.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        prevalidation.toXContent(builder, params);
        return builder.endObject();
    }

}
