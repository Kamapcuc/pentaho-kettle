/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.elasticsearchbulk;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.script.ScriptService;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.elasticsearchbulk.ElasticSearchBulkMeta.OperationType;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Does bulk insert of data into ElasticSearch
 *
 * @author webdetails
 * @since 16-02-2011
 */
public class ElasticSearchBulk extends BaseStep implements StepInterface {

    private static final Charset UTF = Charset.forName("UTF-8");

    private static final String KEY = "params";
    private static final String INSERT_ERROR_CODE = null;
    private static Class<?> PKG = ElasticSearchBulkMeta.class; // for i18n
    private ElasticSearchBulkMeta meta;
    private ElasticSearchBulkData data;

    //TransportClient tc;

    private Node node = null;
    private Client client;
    private boolean externalClient = false;
    private String index;
    private String type;
    private OperationType operationType;

    BulkRequestBuilder currentRequest;

    private int batchSize = 2;

    private boolean isJsonInsert = false;
    private int jsonFieldIdx = 0;

    private String idOutFieldName = null;
    private Integer idFieldIndex = null;
    private Integer parentFieldIndex = null;
    private String script = null;

    private Long timeout = null;
    private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

    //  private long duration = 0L;
    private int numberOfErrors = 0;

    private boolean docAsUpsert = true;
    private boolean stopOnError = true;
    private boolean useOutput = true;

    private Map<String, String> columnsToJson;
    private boolean hasFields;

    private OpType opType = OpType.CREATE;

    private Map<String, Integer> fieldPositions;
    private PreparedMap preparedMap;

    private final ObjectMapper mapper = new ObjectMapper();

    public ElasticSearchBulk(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                             Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    private Integer getFieldPosition(String columnName) throws KettleStepException {
        RowMetaInterface rowMeta = data.inputRowMeta;
        for (int i = 0; i < rowMeta.size(); i++)
            if (idFieldIndex == null || i != idFieldIndex || i != parentFieldIndex) //skip id
                if (columnName.equals(rowMeta.getValueMeta(i).getName()))
                    return i;
        throw new KettleStepException("Column " + columnName + " doesn't found");
    }

    private Map<String, Integer> getFieldPositions() throws KettleException {
        Map<String, Integer> fieldPositions = new HashMap<String, Integer>();
        for (Map.Entry<String, String> jsonEntry : columnsToJson.entrySet())
            fieldPositions.put(jsonEntry.getKey(), getFieldPosition(jsonEntry.getValue()));
        return fieldPositions;
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

        Object[] rowData = getRow();
        if (rowData == null) {
            if (currentRequest != null && currentRequest.numberOfActions() > 0) {
                // didn't fill a whole batch
                processBatch(false);
            }
            setOutputDone();
            return false;
        }

        if (first) {
            first = false;
            setupData();
            fieldPositions = getFieldPositions();
            currentRequest = client.prepareBulk();
            initFieldIndexes();
        }

        try {
            data.inputRowBuffer[data.nextBufferRowIdx++] = rowData;
            return indexRow(rowData);
        } catch (KettleStepException e) {
            throw e;
        } catch (Exception e) {
            rejectAllRows(e.getLocalizedMessage());
            String msg = BaseMessages.getString(PKG, "ElasticSearchBulk.Log.Exception", e.getLocalizedMessage());
            logError(msg);
            throw new KettleStepException(msg, e);
        }
    }

    /**
     * Initialize <code>this.data</code>
     *
     * @throws KettleStepException
     */
    private void setupData() throws KettleStepException {
        data.nextBufferRowIdx = 0;
        data.inputRowMeta = getInputRowMeta().clone();// only available after first getRow();
        data.inputRowBuffer = new Object[batchSize][];
        data.outputRowMeta = data.inputRowMeta.clone();
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);
    }

    private void initFieldIndexes() throws KettleStepException {
        if (isJsonInsert) {
            Integer idx = getFieldIdx(data.inputRowMeta, environmentSubstitute(meta.getJsonField()));
            if (idx != null) {
                jsonFieldIdx = idx;
            } else {
                throw new KettleStepException(BaseMessages.getString(PKG, "ElasticSearchBulk.Error.NoJsonField"));
            }
        }

        idOutFieldName = environmentSubstitute(meta.getIdOutField());

        if (StringUtils.isNotBlank(meta.getIdInField())) {
            idFieldIndex = getFieldIdx(data.inputRowMeta, environmentSubstitute(meta.getIdInField()));
            if (idFieldIndex == null) {
                throw new KettleStepException(BaseMessages.getString(PKG, "ElasticSearchBulk.Error.InvalidIdField"));
            }
        } else idFieldIndex = null;
        if (StringUtils.isNotBlank(meta.getParentInField())) {
            parentFieldIndex = getFieldIdx(data.inputRowMeta, environmentSubstitute(meta.getParentInField()));
            if (parentFieldIndex == null) {
                throw new KettleStepException(BaseMessages.getString(PKG, "ElasticSearchBulk.Error.InvalidParentField"));
            }
        } else parentFieldIndex = null;
    }

    private static Integer getFieldIdx(RowMetaInterface rowMeta, String fieldName) {
        if (fieldName == null) return null;

        for (int i = 0; i < rowMeta.size(); i++) {
            String name = rowMeta.getValueMeta(i).getName();
            if (fieldName.equals(name)) {
                return i;
            }
        }
        return null;
    }

    private boolean indexRow(Object[] row) throws KettleStepException {
        try {
            switch (operationType) {
                case INSERT:
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type);
                    indexRequestBuilder.setOpType(this.opType);
                    if (idFieldIndex != null)
                        indexRequestBuilder.setId(row[idFieldIndex].toString());
                    if (parentFieldIndex != null)
                        indexRequestBuilder.setParent(row[parentFieldIndex].toString());
                    if (isJsonInsert)
                        indexRequestBuilder.setSource(getSourceFromJsonString(row));
                    else
                        indexRequestBuilder.setSource(getSourceFromRowFields(row));
                    currentRequest.add(indexRequestBuilder);
                    break;
                case UPDATE:
                    if (idFieldIndex != null) {
                        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, row[idFieldIndex].toString());
                        if (parentFieldIndex != null)
                            updateRequestBuilder.setParent(row[parentFieldIndex].toString());
                        if (isJsonInsert)
                            updateRequestBuilder.setDoc(getSourceFromJsonString(row));
                        else
                            updateRequestBuilder.setDoc(getSourceFromRowFields(row));
                        updateRequestBuilder.setDocAsUpsert(docAsUpsert);
                        currentRequest.add(updateRequestBuilder);
                        break;
                    } else
                        throw new KettleStepException("Update operation needs to specify id field");
                case SCRIPT:
                    if (idFieldIndex != null) {
                        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, row[idFieldIndex].toString());
                        if (parentFieldIndex != null)
                            updateRequestBuilder.setRouting(row[parentFieldIndex].toString());
                        updateRequestBuilder.setScript(script, ScriptService.ScriptType.INDEXED);
                        Map<String, Object> params;
                        if (isJsonInsert) {
                            params = getMapFromJsonString(row);
                        } else {
                            params = new HashMap<String, Object>();
                            for (Map.Entry<String, Object> entry : getSourceFromRowFields(row).entrySet())
                                params.put(entry.getKey(), entry.getValue());
                        }
                        updateRequestBuilder.setScriptParams(params);
                        currentRequest.add(updateRequestBuilder);
                        break;
                    } else
                        throw new KettleStepException("Script operation needs to specify id field");
                case DELETE:
                    if (idFieldIndex != null) {
                        DeleteRequestBuilder updateRequestBuilder = client.prepareDelete(index, type, row[idFieldIndex].toString());
                        currentRequest.add(updateRequestBuilder);
                        break;
                    } else
                        throw new KettleStepException("Delete operation needs to specify id field");
                default:
                    throw new KettleException("Unrecognized operation type: " + operationType.name());
            }
            return currentRequest.numberOfActions() < batchSize || processBatch(true);
        } catch (KettleStepException e) {
            throw e;
        } catch (NoNodeAvailableException e) {
            throw new KettleStepException(BaseMessages.getString(PKG, "ElasticSearchBulkDialog.Error.NoNodesFound"));
        } catch (Exception e) {
            throw new KettleStepException(BaseMessages.getString(PKG, "ElasticSearchBulk.Log.Exception", e.getLocalizedMessage()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMapFromJsonString(Object[] row) throws KettleStepException {
        String jsonString = (String) row[jsonFieldIdx];
        try {
            return mapper.readValue(jsonString, Map.class);
        } catch (IOException ignored) {
            throw new KettleStepException(BaseMessages.getString("ElasticSearchBulk.Error.NoJsonFieldFormat"));
        }
    }

    private byte[] getSourceFromJsonString(Object[] row) throws KettleStepException {
        Object jsonString = row[jsonFieldIdx];
        if (jsonString instanceof byte[]) {
            return (byte[]) jsonString;
        } else if (jsonString instanceof String) {
            return ((String) jsonString).getBytes(UTF);
        } else
            throw new KettleStepException(BaseMessages.getString("ElasticSearchBulk.Error.NoJsonFieldFormat"));
    }

    private Map<String, Object> getSourceFromRowFields(Object[] row) throws IOException, KettleStepException {
        if (hasFields) {
            for (String key : columnsToJson.keySet()) {
                int index = fieldPositions.get(key);
                preparedMap.putValue(key, row[index]);
            }
            return preparedMap.getMap();
        } else {
            Map<String, Object> result = new HashMap<String, Object>();
            RowMetaInterface rowMeta = data.inputRowMeta;
            for (int i = 0; i < rowMeta.size(); i++) {
                ValueMetaInterface valueMeta = rowMeta.getValueMeta(i);
                String name = valueMeta.getName();
                Object value = row[i];
                result.put(name, value);
            }
            return result;
        }
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (ElasticSearchBulkMeta) smi;
        data = (ElasticSearchBulkData) sdi;
        if (super.init(smi, sdi)) {
            try {
                numberOfErrors = 0;
                initFromMeta();
                initClient();
                return true;
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "ElasticSearchBulk.Log.ErrorOccurredDuringStepInitialize") + e.getMessage()); //$NON-NLS-1$
            }
            return true;
        }
        return false;
    }

    private void initFromMeta() throws KettleStepException {
        index = environmentSubstitute(meta.getIndex());
        type = environmentSubstitute(meta.getType());
        operationType = meta.getOperationType();
        script = meta.getScript();
        batchSize = meta.getBatchSizeInt(this);
        try {
            timeout = Long.parseLong(environmentSubstitute(meta.getTimeOut()));
        } catch (NumberFormatException e) {
            timeout = null;
        }
        timeoutUnit = meta.getTimeoutUnit();
        isJsonInsert = meta.isJsonInsert();
        useOutput = meta.isUseOutput();
        stopOnError = meta.isStopOnError();
        docAsUpsert = meta.isDocAsUpsert();

        columnsToJson = meta.getFields();
        preparedMap = new PreparedMap(columnsToJson);

        this.hasFields = columnsToJson.size() > 0;

        this.opType = StringUtils.isNotBlank(meta.getIdInField()) && meta.isOverWriteIfSameId() ?
                OpType.INDEX :
                OpType.CREATE;
    }

    @Override
    public void setErrors(long e) {
        if (!getStepMeta().isDoingErrorHandling() || stopOnError)
            super.setErrors(e);
    }

    private boolean processBatch(boolean makeNew) {

        ListenableActionFuture<BulkResponse> actionFuture = currentRequest.execute();
        boolean responseOk = false;

        BulkResponse response;

        if (timeout != null && timeoutUnit != null) {
            response = actionFuture.actionGet(timeout, timeoutUnit);
        } else {
            response = actionFuture.actionGet();
        }

        if (response != null) {
            responseOk = handleResponse(response);
        } else {//have to assume all failed
            numberOfErrors += currentRequest.numberOfActions();
            setErrors(numberOfErrors);
        }
        // duration += response.getTookInMillis(); //just in trunk..

        if (makeNew) {
            currentRequest = client.prepareBulk();
            data.nextBufferRowIdx = 0;
            data.inputRowBuffer = new Object[batchSize][];
        } else {
            currentRequest = null;
            data.inputRowBuffer = null;
        }

        return responseOk;
    }

    /**
     * @return <code>true</code> if no errors
     */
    private boolean handleResponse(BulkResponse response) {

        boolean hasErrors = response.hasFailures();

        if (hasErrors && !getStepMeta().isDoingErrorHandling()) {
            logError(response.buildFailureMessage());
        }

        int errorsInBatch = 0;

        if (hasErrors || useOutput) {
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    if (getStepMeta().isDoingErrorHandling()) {
                        rejectRow(item.getItemId(), item.getFailureMessage());
                    } else {
                        logDetailed(item.getFailureMessage());
                        errorsInBatch++;
                    }
                } else if (useOutput) {
                    if (idOutFieldName != null) {
                        addIdToRow(item.getId(), item.getItemId());
                    }
                    echoRow(item.getItemId());
                }
            }
        }

        numberOfErrors += errorsInBatch;
        setErrors(numberOfErrors);
        int linesOK = currentRequest.numberOfActions() - errorsInBatch;

        if (useOutput) setLinesOutput(getLinesOutput() + linesOK);
        else setLinesWritten(getLinesWritten() + linesOK);

        return !hasErrors || !stopOnError;
    }

    private void addIdToRow(String id, int rowIndex) {

        data.inputRowBuffer[rowIndex] = RowDataUtil.resizeArray(data.inputRowBuffer[rowIndex], getInputRowMeta().size() + 1);
        data.inputRowBuffer[rowIndex][getInputRowMeta().size()] = id;

    }

    /**
     * Send input row to output
     */
    private void echoRow(int rowIndex) {
        try {

            putRow(data.outputRowMeta, data.inputRowBuffer[rowIndex]);

        } catch (KettleStepException e) {
            logError(e.getLocalizedMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            logError(e.getLocalizedMessage());
        }
    }

    private void rejectRow(int index, String errorMsg) {
        try {
            putError(getInputRowMeta(), data.inputRowBuffer[index], 1, errorMsg, null, INSERT_ERROR_CODE);

        } catch (KettleStepException e) {
            logError(e.getLocalizedMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            logError(e.getLocalizedMessage());
        }
    }

    private void rejectAllRows(String errorMsg) {
        for (int i = 0; i < data.nextBufferRowIdx; i++) {
            rejectRow(i, errorMsg);
        }
    }

    private Job getRootJob() {
        Job current = getTrans().getParentJob();
        Job previous = null;
        while (current != null) {
            previous = current;
            current = current.getParentJob();
        }
        return previous;
    }

    private void initClient() {

        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(ImmutableSettings.Builder.EMPTY_SETTINGS);//keep default classloader
        settingsBuilder.put(meta.getSettings());
        Settings settings = settingsBuilder.build();

        Job rootJob = getRootJob();
        if (rootJob instanceof ExternalClientJob) {
            client = ((ExternalClientJob) rootJob).getClient();
            externalClient = true;
        } else if (meta.getServers().length > 0) {
            TransportClient tClient = new TransportClient(settings);
            for (InetSocketTransportAddress address : meta.getServersAddresses(this)) {
                tClient.addTransportAddress(address);
            }
            client = tClient;
        } else {
            NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
            nodeBuilder.settings(settings);
            node = nodeBuilder.client(true).node(); // this node will not hold data
            client = node.client();
            node.start();
        }
    }

    private void disposeClient() {
        if (!externalClient) {
            if (client != null) {
                client.close();
            }
            if (node != null) {
                node.close();
            }
        }
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (ElasticSearchBulkMeta) smi;
        data = (ElasticSearchBulkData) sdi;
        try {
            disposeClient();
        } catch (Exception e) {
            logError(e.getLocalizedMessage(), e);
        }
        super.dispose(smi, sdi);
    }
}