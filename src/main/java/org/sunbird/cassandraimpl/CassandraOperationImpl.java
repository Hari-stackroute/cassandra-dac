package org.sunbird.cassandraimpl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Builder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sunbird.cassandra.CassandraOperation;
import org.sunbird.common.CassandraUtil;
import org.sunbird.common.Constants;
import org.sunbird.common.PropertiesCache;
import org.sunbird.common.Response;/*
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.logger;
import org.sunbird.common.models.util.PropertiesCache;
import org.sunbird.common.responsecode.ResponseCode;*/
import org.sunbird.helper.CassandraConnectionManager;
import org.sunbird.helper.CassandraConnectionMngrFactory;

/**
 * @author Amit Kumar
 * @desc this class will hold functions for cassandra db interaction
 */
public class CassandraOperationImpl implements CassandraOperation {
  
  private static Logger logger = LoggerFactory.getLogger(CassandraOperationImpl.class);
  public static final String UNKNOWN_IDENTIFIER = "Unknown identifier ";
  public static final String UNDEFINED_IDENTIFIER = "Undefined column name ";
  public static final String PRIMARY_KEY = "PK";
  public static final String NON_PRIMARY_KEY = "NonPK";
  public static final String ID = "id";
  public static final String SUNBIRD_CASSANDRA_MODE = "sunbird_cassandra_mode";


  private CassandraConnectionManager connectionManager;

  public CassandraOperationImpl() {
    PropertiesCache propertiesCache = PropertiesCache.getInstance();
    String cassandraMode = propertiesCache.getProperty(SUNBIRD_CASSANDRA_MODE);
    connectionManager = CassandraConnectionMngrFactory.getObject(cassandraMode);
  }

  @Override
  public Response insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service insertRecord method started at ==" + startTime);
    Response response = new Response();
    try {
      String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
      PreparedStatement statement = connectionManager.getSession(keyspaceName).prepare(query);
      BoundStatement boundStatement = new BoundStatement(statement);
      Iterator<Object> iterator = request.values().iterator();
      Object[] array = new Object[request.keySet().size()];
      int i = 0;
      while (iterator.hasNext()) {
        array[i++] = iterator.next();
      }
      connectionManager.getSession(keyspaceName).execute(boundStatement.bind(array));
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (Exception e) {
      if (e.getMessage().contains(UNKNOWN_IDENTIFIER)
          || e.getMessage().contains(UNDEFINED_IDENTIFIER)) {
        logger.debug(
            "Exception occured while inserting record to " + tableName + " : " + e.getMessage(), e);
       /* throw new ProjectCommonException(
            ResponseCode.invalidPropertyError.getErrorCode(),
            CassandraUtil.processExceptionForUnknownIdentifier(e),
            ResponseCode.CLIENT_ERROR.getResponseCode());*/
      }
      logger.debug(
          "Exception occured while inserting record to " + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.dbInsertionError.getErrorCode(),
          ResponseCode.dbInsertionError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("insertRecord", startTime);
    return response;
  }

  @Override
  public Response updateRecord(String keyspaceName, String tableName, Map<String, Object> request) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service updateRecord method started at ==" + startTime);
    Response response = new Response();
    try {
      String query = CassandraUtil.getUpdateQueryStatement(keyspaceName, tableName, request);
      PreparedStatement statement = connectionManager.getSession(keyspaceName).prepare(query);
      Object[] array = new Object[request.size()];
      int i = 0;
      String str = "";
      int index = query.lastIndexOf(Constants.SET.trim());
      str = query.substring(index + 4);
      str = str.replace(Constants.EQUAL_WITH_QUE_MARK, "");
      str = str.replace(Constants.WHERE_ID, "");
      str = str.replace(Constants.SEMICOLON, "");
      String[] arr = str.split(",");
      for (String key : arr) {
        array[i++] = request.get(key.trim());
      }
      array[i] = request.get(Constants.IDENTIFIER);
      BoundStatement boundStatement = statement.bind(array);
      connectionManager.getSession(keyspaceName).execute(boundStatement);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (Exception e) {
      if (e.getMessage().contains(UNKNOWN_IDENTIFIER)) {
        logger.debug(Constants.EXCEPTION_MSG_UPDATE + tableName + " : " + e.getMessage(), e);
       /* throw new ProjectCommonException(
            ResponseCode.invalidPropertyError.getErrorCode(),
            CassandraUtil.processExceptionForUnknownIdentifier(e),
            ResponseCode.CLIENT_ERROR.getResponseCode());*/
      }
      logger.debug(Constants.EXCEPTION_MSG_UPDATE + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.dbUpdateError.getErrorCode(),
          ResponseCode.dbUpdateError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("updateRecord", startTime);
    return response;
  }

  @Override
  public Response deleteRecord(String keyspaceName, String tableName, String identifier) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service deleteRecord method started at ==" + startTime);
    Response response = new Response();
    try {
      Delete.Where delete =
          QueryBuilder.delete()
              .from(keyspaceName, tableName)
              .where(QueryBuilder.eq(Constants.IDENTIFIER, identifier));
      connectionManager.getSession(keyspaceName).execute(delete);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_DELETE + tableName + " : " + e.getMessage(), e);
     /* throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("deleteRecord", startTime);
    return response;
  }

  @Override
  public Response getRecordsByProperty(
      String keyspaceName, String tableName, String propertyName, Object propertyValue) {
    return getRecordsByProperty(keyspaceName, tableName, propertyName, propertyValue, null);
  }

  @Override
  public Response getRecordsByProperty(
      String keyspaceName,
      String tableName,
      String propertyName,
      Object propertyValue,
      List<String> fields) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service getRecordsByProperty method started at ==" + startTime);
    Response response = new Response();
    try {
      Session session = connectionManager.getSession(keyspaceName);
      Builder selectBuilder;
      if (CollectionUtils.isNotEmpty(fields)) {
        selectBuilder = QueryBuilder.select((String[]) fields.toArray());
      } else {
        selectBuilder = QueryBuilder.select().all();
      }
      Statement selectStatement =
          selectBuilder
              .from(keyspaceName, tableName)
              .where(QueryBuilder.eq(propertyName, propertyValue));
      ResultSet results = null;
      results = session.execute(selectStatement);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByProperty", startTime);
    return response;
  }

  @Override
  public Response getRecordsByProperty(
      String keyspaceName, String tableName, String propertyName, List<Object> propertyValueList) {
    return getRecordsByProperty(keyspaceName, tableName, propertyName, propertyValueList, null);
  }

  @Override
  public Response getRecordsByProperty(
      String keyspaceName,
      String tableName,
      String propertyName,
      List<Object> propertyValueList,
      List<String> fields) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service getRecordsByProperty method started at ==" + startTime);
    Response response = new Response();
    try {
      Builder selectBuilder;
      if (CollectionUtils.isNotEmpty(fields)) {
        selectBuilder = QueryBuilder.select(fields.toArray(new String[fields.size()]));
      } else {
        selectBuilder = QueryBuilder.select().all();
      }
      Statement selectStatement =
          selectBuilder
              .from(keyspaceName, tableName)
              .where(QueryBuilder.in(propertyName, propertyValueList));
      ResultSet results = connectionManager.getSession(keyspaceName).execute(selectStatement);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByProperty", startTime);
    return response;
  }

  @Override
  public Response getRecordsByProperties(
      String keyspaceName, String tableName, Map<String, Object> propertyMap) {
    return getRecordsByProperties(keyspaceName, tableName, propertyMap, null);
  }

  @Override
  public Response getRecordsByProperties(
      String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "Cassandra Service getRecordsByProperties method started at ==" + startTime);
    Response response = new Response();
    try {
      Builder selectBuilder;
      if (CollectionUtils.isNotEmpty(fields)) {
        String[] dbFields = fields.toArray(new String[fields.size()]);
        selectBuilder = QueryBuilder.select(dbFields);
      } else {
        selectBuilder = QueryBuilder.select().all();
      }
      Select selectQuery = selectBuilder.from(keyspaceName, tableName);
      Where selectWhere = selectQuery.where();
      for (Entry<String, Object> entry : propertyMap.entrySet()) {
        if (entry.getValue() instanceof List) {
          List<Object> list = (List) entry.getValue();
          if (null != list) {
            Object[] propertyValues = list.toArray(new Object[list.size()]);
            Clause clause = QueryBuilder.in(entry.getKey(), propertyValues);
            selectWhere.and(clause);
          }
        } else {
          Clause clause = QueryBuilder.eq(entry.getKey(), entry.getValue());
          selectWhere.and(clause);
        }
      }
      ResultSet results =
          connectionManager.getSession(keyspaceName).execute(selectQuery.allowFiltering());
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByProperties", startTime);
    return response;
  }

  @Override
  public Response getPropertiesValueById(
      String keyspaceName, String tableName, String id, String... properties) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "Cassandra Service getPropertiesValueById method started at ==" + startTime);
    Response response = new Response();
    try {
      String selectQuery = CassandraUtil.getSelectStatement(keyspaceName, tableName, properties);
      PreparedStatement statement = connectionManager.getSession(keyspaceName).prepare(selectQuery);
      BoundStatement boundStatement = new BoundStatement(statement);
      ResultSet results =
          connectionManager.getSession(keyspaceName).execute(boundStatement.bind(id));
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getPropertiesValueById", startTime);
    return response;
  }

  @Override
  public Response getAllRecords(String keyspaceName, String tableName) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "Cassandra Service getAllRecords method started at ==" + startTime);
    Response response = new Response();
    try {
      Select selectQuery = QueryBuilder.select().all().from(keyspaceName, tableName);
      ResultSet results = connectionManager.getSession(keyspaceName).execute(selectQuery);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getAllRecords", startTime);
    return response;
  }

  @Override
  public Response upsertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service upsertRecord method started at ==" + startTime);
    Response response = new Response();
    try {
      String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
      PreparedStatement statement = connectionManager.getSession(keyspaceName).prepare(query);
      BoundStatement boundStatement = new BoundStatement(statement);
      Iterator<Object> iterator = request.values().iterator();
      Object[] array = new Object[request.keySet().size()];
      int i = 0;
      while (iterator.hasNext()) {
        array[i++] = iterator.next();
      }
      connectionManager.getSession(keyspaceName).execute(boundStatement.bind(array));
      response.put(Constants.RESPONSE, Constants.SUCCESS);

    } catch (Exception e) {
      if (e.getMessage().contains(UNKNOWN_IDENTIFIER)) {
        logger.error(Constants.EXCEPTION_MSG_UPSERT + tableName + " : " + e.getMessage(), e);
        /*throw new ProjectCommonException(
            ResponseCode.invalidPropertyError.getErrorCode(),
            CassandraUtil.processExceptionForUnknownIdentifier(e),
            ResponseCode.CLIENT_ERROR.getResponseCode());*/
      }
      logger.error(Constants.EXCEPTION_MSG_UPSERT + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("upsertRecord", startTime);
    return response;
  }

  @Override
  public Response updateRecord(
      String keyspaceName,
      String tableName,
      Map<String, Object> request,
      Map<String, Object> compositeKey) {

    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service updateRecord method started at ==" + startTime);
    Response response = new Response();
    try {
      Session session = connectionManager.getSession(keyspaceName);
      Update update = QueryBuilder.update(keyspaceName, tableName);
      Assignments assignments = update.with();
      Update.Where where = update.where();
      request
          .entrySet()
          .stream()
          .forEach(
              x -> {
                assignments.and(QueryBuilder.set(x.getKey(), x.getValue()));
              });
      compositeKey
          .entrySet()
          .stream()
          .forEach(
              x -> {
                where.and(QueryBuilder.eq(x.getKey(), x.getValue()));
              });
      Statement updateQuery = where;
      session.execute(updateQuery);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_UPDATE + tableName + " : " + e.getMessage(), e);
      /*if (e.getMessage().contains(UNKNOWN_IDENTIFIER)) {
        throw new ProjectCommonException(
            ResponseCode.invalidPropertyError.getErrorCode(),
            CassandraUtil.processExceptionForUnknownIdentifier(e),
            ResponseCode.CLIENT_ERROR.getResponseCode());
      }*/
      /*throw new ProjectCommonException(
          ResponseCode.dbUpdateError.getErrorCode(),
          ResponseCode.dbUpdateError.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("updateRecord", startTime);
    return response;
  }

  private Response getRecordByIdentifier(
      String keyspaceName, String tableName, Object key, List<String> fields) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "Cassandra Service getRecordBy key method started at ==" + startTime);
    Response response = new Response();
    try {
      Session session = connectionManager.getSession(keyspaceName);
      Builder selectBuilder;
      if (CollectionUtils.isNotEmpty(fields)) {
        selectBuilder = QueryBuilder.select((String[]) fields.toArray());
      } else {
        selectBuilder = QueryBuilder.select().all();
      }
      Select selectQuery = selectBuilder.from(keyspaceName, tableName);
      Where selectWhere = selectQuery.where();
      if (key instanceof String) {
        selectWhere.and(QueryBuilder.eq(Constants.IDENTIFIER, key));
      } else if (key instanceof Map) {
        Map<String, Object> compositeKey = (Map<String, Object>) key;
        compositeKey
            .entrySet()
            .stream()
            .forEach(
                x -> {
                  CassandraUtil.createQuery(x.getKey(), x.getValue(), selectWhere);
                });
      }
      ResultSet results = session.execute(selectWhere);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
     /* throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordByIdentifier", startTime);
    return response;
  }

  @Override
  public Response getRecordById(String keyspaceName, String tableName, String key) {
    return getRecordByIdentifier(keyspaceName, tableName, key, null);
  }

  @Override
  public Response getRecordById(String keyspaceName, String tableName, Map<String, Object> key) {
    return getRecordByIdentifier(keyspaceName, tableName, key, null);
  }

  @Override
  public Response getRecordById(
      String keyspaceName, String tableName, String key, List<String> fields) {
    return getRecordByIdentifier(keyspaceName, tableName, key, fields);
  }

  @Override
  public Response getRecordById(
      String keyspaceName, String tableName, Map<String, Object> key, List<String> fields) {
    return getRecordByIdentifier(keyspaceName, tableName, key, fields);
  }

  /*@Override
  public Response batchInsert(
      String keyspaceName, String tableName, List<Map<String, Object>> records) {

    long startTime = System.currentTimeMillis();
    logger.info(
        "Cassandra Service batchInsert method started at ==" + startTime);

    Session session = connectionManager.getSession(keyspaceName);
    Response response = new Response();
    BatchStatement batchStatement = new BatchStatement();
    ResultSet resultSet = null;

    try {
      for (Map<String, Object> map : records) {
        Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
        map.entrySet()
            .stream()
            .forEach(
                x -> {
                  insert.value(x.getKey(), x.getValue());
                });
        batchStatement.add(insert);
      }
      resultSet = session.execute(batchStatement);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (QueryExecutionException
        | QueryValidationException
        | NoHostAvailableException
        | IllegalStateException e) {
      logger.error("Cassandra Batch Insert Failed." + e.getMessage(), e);
      *//*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*//*
    }
    logQueryElapseTime("batchInsert", startTime);
    return response;
  }*/

  @Override
  public Response batchUpdate(
      String keyspaceName, String tableName, List<Map<String, Map<String, Object>>> list) {

    BatchStatement batchStatement = new BatchStatement();
    long startTime = System.currentTimeMillis();
    logger.info(
        "Cassandra Service batchUpdate method started at ==" + startTime);
    Response response = new Response();
    ResultSet resultSet = null;
    try {
      Session session = connectionManager.getSession(keyspaceName);
      for (Map<String, Map<String, Object>> record : list) {
        Map<String, Object> primaryKey = record.get(PRIMARY_KEY);
        Map<String, Object> nonPKRecord = record.get(NON_PRIMARY_KEY);
        batchStatement.add(
            CassandraUtil.createUpdateQuery(primaryKey, nonPKRecord, keyspaceName, tableName));
      }
      resultSet = session.execute(batchStatement);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (Exception ex) {
      logger.error("Cassandra Batch Update failed " + ex.getMessage(), ex);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("batchUpdate", startTime);
    return response;
  }

  private void logQueryElapseTime(String operation, long startTime) {

    long stopTime = System.currentTimeMillis();
    long elapsedTime = stopTime - startTime;
    String message =
        "Cassandra operation {0} started at {1} and completed at {2}. Total time elapsed is {3}.";
    MessageFormat mf = new MessageFormat(message);
    logger.debug(
        mf.format(new Object[] {operation, startTime, stopTime, elapsedTime}));
  }

  @Override
  public Response getRecordsByIndexedProperty(
      String keyspaceName, String tableName, String propertyName, Object propertyValue) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "CassandraOperationImpl:getRecordsByIndexedProperty called at " + startTime);
    Response response = new Response();
    try {
      Select selectQuery = QueryBuilder.select().all().from(keyspaceName, tableName);
      selectQuery.where().and(QueryBuilder.eq(propertyName, propertyValue));
      ResultSet results =
          connectionManager.getSession(keyspaceName).execute(selectQuery.allowFiltering());
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.error(
          "CassandraOperationImpl:getRecordsByIndexedProperty: "
              + Constants.EXCEPTION_MSG_FETCH
              + tableName
              + " : "
              + e.getMessage(),
          e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByIndexedProperty", startTime);
    return response;
  }

  @Override
  public void deleteRecord(
      String keyspaceName, String tableName, Map<String, String> compositeKeyMap) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "CassandraOperationImpl: deleteRecord by composite key called at " + startTime);
    try {
      Delete delete = QueryBuilder.delete().from(keyspaceName, tableName);
      Delete.Where deleteWhere = delete.where();
      compositeKeyMap
          .entrySet()
          .stream()
          .forEach(
              x -> {
                Clause clause = QueryBuilder.eq(x.getKey(), x.getValue());
                deleteWhere.and(clause);
              });
      connectionManager.getSession(keyspaceName).execute(delete);
    } catch (Exception e) {
      logger.error(
          "CassandraOperationImpl: deleteRecord by composite key. "
              + Constants.EXCEPTION_MSG_DELETE
              + tableName
              + " : "
              + e.getMessage(),
          e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("deleteRecordByCompositeKey", startTime);
  }

  @Override
  public boolean deleteRecords(String keyspaceName, String tableName, List<String> identifierList) {
    long startTime = System.currentTimeMillis();
    ResultSet resultSet = null;
    logger.debug(
        "CassandraOperationImpl: deleteRecords called at " + startTime);
    try {
      Delete delete = QueryBuilder.delete().from(keyspaceName, tableName);
      Delete.Where deleteWhere = delete.where();
      Clause clause = QueryBuilder.in(ID, identifierList);
      deleteWhere.and(clause);
      resultSet = connectionManager.getSession(keyspaceName).execute(delete);
    } catch (Exception e) {
      logger.debug(
          "CassandraOperationImpl: deleteRecords by list of primary key. "
              + Constants.EXCEPTION_MSG_DELETE
              + tableName
              + " : "
              + e.getMessage(),
          e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("deleteRecords", startTime);
    return resultSet.wasApplied();
  }

  @Override
  public Response getRecordsByCompositeKey(
      String keyspaceName, String tableName, Map<String, Object> compositeKeyMap) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "CassandraOperationImpl: getRecordsByCompositeKey called at " + startTime);
    Response response = new Response();
    try {
      Builder selectBuilder = QueryBuilder.select().all();
      Select selectQuery = selectBuilder.from(keyspaceName, tableName);
      Where selectWhere = selectQuery.where();
      for (Entry<String, Object> entry : compositeKeyMap.entrySet()) {
        Clause clause = QueryBuilder.eq(entry.getKey(), entry.getValue());
        selectWhere.and(clause);
      }
      ResultSet results = connectionManager.getSession(keyspaceName).execute(selectQuery);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.error(
          "CassandraOperationImpl:getRecordsByCompositeKey: "
              + Constants.EXCEPTION_MSG_FETCH
              + tableName
              + " : "
              + e.getMessage());
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByCompositeKey", startTime);
    return response;
  }

  @Override
  public Response getRecordsByIdsWithSpecifiedColumns(
      String keyspaceName, String tableName, List<String> properties, List<String> ids) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "CassandraOperationImpl: getRecordsByIdsWithSpecifiedColumns call started at " + startTime);
    Response response = new Response();
    try {
      Builder selectBuilder;
      if (CollectionUtils.isNotEmpty(properties)) {
        selectBuilder = QueryBuilder.select(properties.toArray(new String[properties.size()]));
      } else {
        selectBuilder = QueryBuilder.select().all();
      }
      response = executeSelectQuery(keyspaceName, tableName, ids, selectBuilder, "");
    } catch (Exception e) {
      logger.debug(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByIdsWithSpecifiedColumns", startTime);
    return response;
  }

  private Response executeSelectQuery(
      String keyspaceName,
      String tableName,
      List<String> ids,
      Builder selectBuilder,
      String primaryKeyColumnName) throws Exception {
    Response response;
    Select selectQuery = selectBuilder.from(keyspaceName, tableName);
    Where selectWhere = selectQuery.where();
    Clause clause = null;
    if (StringUtils.isBlank(primaryKeyColumnName)) {
      clause = QueryBuilder.in(ID, ids.toArray(new Object[ids.size()]));
    } else {
      clause = QueryBuilder.in(primaryKeyColumnName, ids.toArray(new Object[ids.size()]));
    }

    selectWhere.and(clause);
    ResultSet results = connectionManager.getSession(keyspaceName).execute(selectQuery);
    response = CassandraUtil.createResponse(results);
    return response;
  }

  @Override
  public Response getRecordsByPrimaryKeys(
      String keyspaceName,
      String tableName,
      List<String> primaryKeys,
      String primaryKeyColumnName) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "CassandraOperationImpl: getRecordsByPrimaryKeys call started at " + startTime);
    Response response = new Response();
    try {
      Builder selectBuilder = QueryBuilder.select().all();
      response =
          executeSelectQuery(
              keyspaceName, tableName, primaryKeys, selectBuilder, primaryKeyColumnName);
    } catch (Exception e) {
      logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
      /*throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByPrimaryKeys", startTime);
    return response;
  }

  @Override
  public Response insertRecordWithTTL(
      String keyspaceName, String tableName, Map<String, Object> request, int ttl) throws Exception {
    long startTime = System.currentTimeMillis();
    Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
    request
        .entrySet()
        .stream()
        .forEach(
            x -> {
              insert.value(x.getKey(), x.getValue());
            });
    insert.using(QueryBuilder.ttl(ttl));
    logger.info(
        "CassandraOperationImpl:insertRecordWithTTL: query = " + insert.getQueryString());
    ResultSet results = connectionManager.getSession(keyspaceName).execute(insert);
    Response response = CassandraUtil.createResponse(results);
    logQueryElapseTime("insertRecordWithTTL", startTime);
    return response;
  }

  @Override
  public Response getRecordsByIdsWithSpecifiedColumnsAndTTL(
      String keyspaceName,
      String tableName,
      Map<String, Object> primaryKeys,
      List<String> properties,
      Map<String, String> ttlPropertiesWithAlias) {
    long startTime = System.currentTimeMillis();
    logger.info(
        "CassandraOperationImpl:getRecordsByIdsWithSpecifiedColumnsAndTTL: call started at "
            + startTime);
    Response response = new Response();
    try {

      Selection selection = QueryBuilder.select();

      if (CollectionUtils.isNotEmpty(properties)) {
        properties
            .stream()
            .forEach(
                property -> {
                  selection.column(property);
                });
      }

      if (MapUtils.isNotEmpty(ttlPropertiesWithAlias)) {
        ttlPropertiesWithAlias
            .entrySet()
            .stream()
            .forEach(
                property -> {
                  if (StringUtils.isBlank(property.getValue())) {
                    logger.error(
                        "CassandraOperationImpl:getRecordsByIdsWithSpecifiedColumnsAndTTL: Alias not provided for ttl key = "
                            + property.getKey());
                    //ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR);
                    //throw new Exception();
                  }
                  selection.ttl(property.getKey()).as(property.getValue());
                });
      }
      Select select = selection.from(keyspaceName, tableName);
      primaryKeys
          .entrySet()
          .stream()
          .forEach(
              primaryKey -> {
                select.where().and(QueryBuilder.eq(primaryKey.getKey(), primaryKey.getValue()));
              });
      logger.debug("Query =" + select.getQueryString());
      ResultSet results = connectionManager.getSession(keyspaceName).execute(select);
      response = CassandraUtil.createResponse(results);
    } catch (Exception e) {
      logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
     /* throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());*/
    }
    logQueryElapseTime("getRecordsByIdsWithSpecifiedColumnsAndTTL", startTime);
    return response;
  }

  /*@Override
  public Response batchInsertWithTTL(
      String keyspaceName,
      String tableName,
      List<Map<String, Object>> records,
      List<Integer> ttls) {
    long startTime = System.currentTimeMillis();
    logger.debug(
        "CassandraOperationImpl:batchInsertWithTTL: call started at "
            + startTime,
        LoggerEnum.INFO);
    if (CollectionUtils.isEmpty(records) || CollectionUtils.isEmpty(ttls)) {
      logger.error(
          "CassandraOperationImpl:batchInsertWithTTL: records or ttls is empty");
      ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR);
    }
    if (ttls.size() != records.size()) {
      logger.debug(
          "CassandraOperationImpl:batchInsertWithTTL: Mismatch of records and ttls list size");
      ProjectCommonException.throwServerErrorException(ResponseCode.SERVER_ERROR);
    }
    Session session = connectionManager.getSession(keyspaceName);
    Response response = new Response();
    BatchStatement batchStatement = new BatchStatement();
    ResultSet resultSet = null;
    Iterator<Integer> ttlIterator = ttls.iterator();
    try {
      for (Map<String, Object> map : records) {
        Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
        map.entrySet()
            .stream()
            .forEach(
                x -> {
                  insert.value(x.getKey(), x.getValue());
                });
        if (ttlIterator.hasNext()) {
          Integer ttlVal = ttlIterator.next();
          if (ttlVal != null & ttlVal > 0) {
            insert.using(QueryBuilder.ttl(ttlVal));
          }
        }
        batchStatement.add(insert);
      }
      resultSet = session.execute(batchStatement);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
    } catch (QueryExecutionException
        | QueryValidationException
        | NoHostAvailableException
        | IllegalStateException e) {
      logger.debug("CassandraOperationImpl:batchInsertWithTTL: Exception occurred with error message = " + e.getMessage(), e);
      throw new ProjectCommonException(
          ResponseCode.SERVER_ERROR.getErrorCode(),
          ResponseCode.SERVER_ERROR.getErrorMessage(),
          ResponseCode.SERVER_ERROR.getResponseCode());
    }
    logQueryElapseTime("batchInsertWithTTL", startTime);
    return response;
  }*/

}
