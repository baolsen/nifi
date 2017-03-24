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

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.logging.ComponentLog;
import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.put.PutColumn;

// import java.nio.charset.Charset;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Tags({"dedupe", "dupe", "duplicate", "hash", "hbase", "get"})
@CapabilityDescription(
   "Using HBase, caches a value, computed from FlowFile attributes, for each incoming FlowFile and determines if the cached value has already been seen. " +
   "If so, routes the file to 'duplicate'. " +
   "Otherwise, routes the file to 'non-duplicate'. " +
   "Useful as an HBase-based substitute for the DetectDuplicate processor." 
)
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class DetectDuplicateUsingHBase extends AbstractProcessor {

    public static final AllowableValue UPDATE_CACHE_ALWAYS = new AllowableValue("Always", "Always",
	"Cache will always be updated, whether or not an entry exists already.");

    public static final AllowableValue UPDATE_CACHE_NEW = new AllowableValue("Non-duplicate", "Non-duplicate",
        "Cache will be updated only if an entry does NOT exist already.");

    public static final AllowableValue UPDATE_CACHE_NEVER = new AllowableValue("Never", "Never",
        "processor would only check for duplicates and not cache the Entry Identifier, requiring another " +
        "processor to add identifiers to the HBase cache.");

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
        .name("Cache Entry Identifier")
        .description(
            "A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated"
            + " against a FlowFile in order to determine the value used to identify duplicates."
            + " This value will be cached, meaning it will be used as the HBase row key.")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
        .defaultValue("${hash.value}")
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor UPDATE_CACHE_METHOD = new PropertyDescriptor.Builder()
        .name("Update cache method")
        .description("Controls when the cache should be updated. ") 
        .required(true)
        .allowableValues(UPDATE_CACHE_ALWAYS, UPDATE_CACHE_NEW, UPDATE_CACHE_NEVER)
        .defaultValue(UPDATE_CACHE_ALWAYS.getValue())
        .build();

    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("HBase Client Service")
        .description("Specifies the Controller Service to use for accessing HBase.")
        .required(true)
        .identifiesControllerService(HBaseClientService.class)
        .build();

    public static final PropertyDescriptor HBASE_TABLE_NAME = new PropertyDescriptor.Builder()
        .name("HBase table name")
        .description("Name of the HBase table to use for the cache. Must exist.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor COLUMN_MAPPING = new PropertyDescriptor.Builder()
        .name("HBase column mapping")
        .description(
            "Comma-separated list which maps HBase columns to FlowFile attributes. Each element in the list is of the form" 
            + " <columnFamily>:<columnQualifier>=<value>. The list may contain Attribute Expression Language statements" 
            + " , which will be evaluated against a FlowFile." 
            + " Each element is stored in HBase against the Cache Entry Identifier for the FlowFile.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue("f:filename=${filename},f:uuid=${uuid}")
        .build();

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
        descriptors.add(UPDATE_CACHE_METHOD);
        descriptors.add(HBASE_CLIENT_SERVICE);
        descriptors.add(HBASE_TABLE_NAME);
        descriptors.add(COLUMN_MAPPING);
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

        // Get the specified flowfile attribute which is used as the file ID
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
       
        final String tableName = context.getProperty(HBASE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class); 
      
        // Check the HBase cache table for the file ID 
        final RowCountHandler handler = new RowCountHandler();

        final String rowId=cacheKey;
        final byte[] rowIdBytes=rowId.getBytes(StandardCharsets.UTF_8);

        final List<Column> columnsList = new ArrayList<Column>(0);

	try {
            hBaseClientService.scan(tableName, rowIdBytes, rowIdBytes, columnsList, handler);
        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            logger.error("Unable to fetch row {} from  {} due to {}", new Object[] {rowId, tableName, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final boolean duplicate = (handler.handledRow > 0);

        // Add the file ID to the HBase cache table (if required)
        final String updateCacheMethod = context.getProperty(UPDATE_CACHE_METHOD).getValue();
        final String columnMapping = context.getProperty(COLUMN_MAPPING).evaluateAttributeExpressions(flowFile).getValue();

        final boolean updateCache = ( UPDATE_CACHE_ALWAYS.equals(updateCacheMethod) || (UPDATE_CACHE_NEW.equals(updateCacheMethod) && !duplicate));
        try {

            if (updateCache) updateHBaseCache(hBaseClientService, tableName, rowIdBytes, columnMapping);

        } catch (Exception e) {
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            logger.error("Unable to update HBase table {} row {} due to {}", new Object[] {tableName, rowId, e});
            session.transfer(flowFile, REL_FAILURE);
        }

        if (duplicate) {
            // TODO originalFlowFileDescription
            session.getProvenanceReporter().route(flowFile, REL_DUPLICATE);
            session.transfer(flowFile, REL_DUPLICATE);
            logger.info("Found {} to be a duplicate of FlowFile ", new Object[]{flowFile});
            session.adjustCounter("Duplicates Detected", 1L, false);
        }
        else {
            session.getProvenanceReporter().route(flowFile, REL_NON_DUPLICATE);
            session.transfer(flowFile, REL_NON_DUPLICATE);
            logger.info("Could not find a duplicate entry in cache for {}; routing to non-duplicate", new Object[]{flowFile});
            session.adjustCounter("Non-Duplicate Files Processed", 1L, false);
        }
    }

    private void updateHBaseCache(HBaseClientService hBaseClientService, String tableName, byte[] rowIdBytes, String columnMapping) throws IOException {
        final String[] mappings = (columnMapping == null || columnMapping.isEmpty() ? new String[0] : columnMapping.split(","));

        List<PutColumn> putColumns = new ArrayList<PutColumn>(mappings.length);

        for (final String mapping : mappings) {
            final String[] kv = mapping.split("=");
            final String[] fq = kv[0].split(":");
           
            final byte[] columnFamilyBytes = fq[0].getBytes(StandardCharsets.UTF_8);
            final byte[] columnQualifierBytes = fq[1].getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = kv[1].getBytes(StandardCharsets.UTF_8);

            final PutColumn putColumn = new PutColumn(columnFamilyBytes, columnQualifierBytes, valueBytes);
            putColumns.add(putColumn);
        }

        hBaseClientService.put(tableName, rowIdBytes, putColumns);
    }

    private static class RowCountHandler implements ResultHandler {
        private int handledRow = 0;

        @Override
        public void handle(byte[] row, ResultCell[] resultCells) {
            handledRow += 1;
        }
        public int handledRow() {
            return handledRow;
        }

    }

}
