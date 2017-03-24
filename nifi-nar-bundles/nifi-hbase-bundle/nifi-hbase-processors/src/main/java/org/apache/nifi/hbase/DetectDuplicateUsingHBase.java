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
package org.apache.nifi.hbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.scan.Column;

// import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class DetectDuplicateUsingHBase extends AbstractProcessor {

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description(
                    "A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated "
                    + "against a FlowFile in order to determine the value used to identify duplicates; it is this value that is cached")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${hash.value}")
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    public static final PropertyDescriptor HBASE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("HBase table name")
            .description(
                    "Name of the HBase table to use for the cache ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
/*
    public static final PropertyDescriptor HBASE_COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("HBase column family")
            .description(
                    "Name of the HBase column family to use for the cache ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
*/
    public static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("If a FlowFile has been detected to be a duplicate, it will be routed to this relationship")
            .build();

    public static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("If a FlowFile's Cache Entry Identifier was not found in the cache, it will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CACHE_ENTRY_IDENTIFIER);
        // descriptors.add(HBASE_TABLE_NAME);
        // descriptors.add(HBASE_COLUMN_FAMILY);
        descriptors.add(HBASE_CLIENT_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_DUPLICATE);
        relationships.add(REL_NON_DUPLICATE);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final ComponentLog logger = getLogger();

        // Get the specified flowfile attribute
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
       
        final String tableName = context.getProperty(HBASE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class); 
       
        final RowHandler handler = new RowHandler();

        final String rowId="";
        final byte[] rowIdBytes=rowId.getBytes(StandardCharsets.UTF_8);

        final List<Column> columnsList = new ArrayList<Column>(0);

	try {
            hBaseClientService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
        } catch (Exception e) {
            getLogger().error("Unable to fetch row {} from  {} due to {}", new Object[] {rowId, tableName, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

// Below here wont work
        /*
        final String originalCacheValue;
 
        final boolean shouldCacheIdentifier = context.getProperty(CACHE_IDENTIFIER).asBoolean();
        if (shouldCacheIdentifier) {
                originalCacheValue = cache.getAndPutIfAbsent(cacheKey, cacheValue, keySerializer, valueSerializer, valueDeserializer);
        } else {
                originalCacheValue = cache.get(cacheKey, keySerializer, valueDeserializer);
        }*/
 
    }

    private static class RowHandler implements ResultHandler {
        private boolean handledRow = false;

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            handledRow = true;
        }
        public boolean handledRow() {
            return handledRow;
        }

    }

}
