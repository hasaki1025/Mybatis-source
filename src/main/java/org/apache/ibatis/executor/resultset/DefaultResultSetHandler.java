/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor.resultset;

import java.lang.reflect.Constructor;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.annotations.AutomapConstructor;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.cursor.defaults.DefaultCursor;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.loader.ResultLoader;
import org.apache.ibatis.executor.loader.ResultLoaderMap;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.executor.result.DefaultResultHandler;
import org.apache.ibatis.executor.result.ResultMapException;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultContext;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Iwao AVE!
 * @author Kazuki Shimizu
 */
public class DefaultResultSetHandler implements ResultSetHandler {

  private static final Object DEFERRED = new Object();

  private final Executor executor;
  private final Configuration configuration;
  private final MappedStatement mappedStatement;
  private final RowBounds rowBounds;
  private final ParameterHandler parameterHandler;
  private final ResultHandler<?> resultHandler;
  private final BoundSql boundSql;
  private final TypeHandlerRegistry typeHandlerRegistry;
  private final ObjectFactory objectFactory;
  private final ReflectorFactory reflectorFactory;

  // nested resultmaps
  private final Map<CacheKey, Object> nestedResultObjects = new HashMap<>();
  private final Map<String, Object> ancestorObjects = new HashMap<>();
  private Object previousRowValue;

  // multiple resultsets
  private final Map<String, ResultMapping> nextResultMaps = new HashMap<>();
  private final Map<CacheKey, List<PendingRelation>> pendingRelations = new HashMap<>();

  // Cached Automappings
  private final Map<String, List<UnMappedColumnAutoMapping>> autoMappingsCache = new HashMap<>();

  // temporary marking flag that indicate using constructor mapping (use field to reduce memory usage)
  private boolean useConstructorMappings;

  private static class PendingRelation {
    public MetaObject metaObject;
    public ResultMapping propertyMapping;
  }

  private static class UnMappedColumnAutoMapping {
    private final String column;
    private final String property;
    private final TypeHandler<?> typeHandler;
    private final boolean primitive;

    public UnMappedColumnAutoMapping(String column, String property, TypeHandler<?> typeHandler, boolean primitive) {
      this.column = column;
      this.property = property;
      this.typeHandler = typeHandler;
      this.primitive = primitive;
    }
  }

  public DefaultResultSetHandler(Executor executor, MappedStatement mappedStatement, ParameterHandler parameterHandler, ResultHandler<?> resultHandler, BoundSql boundSql,
                                 RowBounds rowBounds) {
    this.executor = executor;
    this.configuration = mappedStatement.getConfiguration();
    this.mappedStatement = mappedStatement;
    this.rowBounds = rowBounds;
    this.parameterHandler = parameterHandler;
    this.boundSql = boundSql;
    this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
    this.objectFactory = configuration.getObjectFactory();
    this.reflectorFactory = configuration.getReflectorFactory();
    this.resultHandler = resultHandler;
  }

  //
  // HANDLE OUTPUT PARAMETER
  //

  @Override
  public void handleOutputParameters(CallableStatement cs) throws SQLException {
    final Object parameterObject = parameterHandler.getParameterObject();
    final MetaObject metaParam = configuration.newMetaObject(parameterObject);
    final List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    for (int i = 0; i < parameterMappings.size(); i++) {
      final ParameterMapping parameterMapping = parameterMappings.get(i);
      if (parameterMapping.getMode() == ParameterMode.OUT || parameterMapping.getMode() == ParameterMode.INOUT) {
        if (ResultSet.class.equals(parameterMapping.getJavaType())) {
          handleRefCursorOutputParameter((ResultSet) cs.getObject(i + 1), parameterMapping, metaParam);
        } else {
          final TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
          metaParam.setValue(parameterMapping.getProperty(), typeHandler.getResult(cs, i + 1));
        }
      }
    }
  }

  private void handleRefCursorOutputParameter(ResultSet rs, ParameterMapping parameterMapping, MetaObject metaParam) throws SQLException {
    if (rs == null) {
      return;
    }
    try {
      final String resultMapId = parameterMapping.getResultMapId();
      final ResultMap resultMap = configuration.getResultMap(resultMapId);
      final ResultSetWrapper rsw = new ResultSetWrapper(rs, configuration);
      if (this.resultHandler == null) {
        final DefaultResultHandler resultHandler = new DefaultResultHandler(objectFactory);
        handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
        metaParam.setValue(parameterMapping.getProperty(), resultHandler.getResultList());
      } else {
        handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
      }
    } finally {
      // issue #228 (close resultsets)
      closeResultSet(rs);
    }
  }

  //
  // HANDLE RESULT SETS
  //
  @Override
  public List<Object> handleResultSets(Statement stmt) throws SQLException {
    ErrorContext.instance().activity("handling results").object(mappedStatement.getId());

    final List<Object> multipleResults = new ArrayList<>();

    int resultSetCount = 0;//结果条数
    ResultSetWrapper rsw = getFirstResultSet(stmt);//获取ResultSetWrapper

    List<ResultMap> resultMaps = mappedStatement.getResultMaps();//获取该mappedStatement需要的ResultMap数量
    int resultMapCount = resultMaps.size();//返回的结果条数
    validateResultMapsCount(rsw, resultMapCount);//校验ResultMap的数量至少为1（在指定了ResultMap的情况下）
    while (rsw != null && resultMapCount > resultSetCount) {//resultMapCount > resultSetCount保证ResultMap的数量需要ResultSet的数量
      ResultMap resultMap = resultMaps.get(resultSetCount);//获取ResultMap
      handleResultSet(rsw, resultMap, multipleResults, null);//解析返回结果并将结果放入multipleResults中
      rsw = getNextResultSet(stmt);
      cleanUpAfterHandlingResultSet();
      resultSetCount++;//已解析的ResultSet数量加一
    }

    String[] resultSets = mappedStatement.getResultSets();//再次获取ResultSet（正常流程此时ResultSet已经关闭）
    if (resultSets != null) {//嵌套查询才会出现此时resultSets不为空的情况
      while (rsw != null && resultSetCount < resultSets.length) {
        ResultMapping parentMapping = nextResultMaps.get(resultSets[resultSetCount]);
        if (parentMapping != null) {
          String nestedResultMapId = parentMapping.getNestedResultMapId();
          ResultMap resultMap = configuration.getResultMap(nestedResultMapId);
          handleResultSet(rsw, resultMap, null, parentMapping);
        }
        rsw = getNextResultSet(stmt);
        cleanUpAfterHandlingResultSet();
        resultSetCount++;
      }
    }

    return collapseSingleResultList(multipleResults);//如果返回结果的数量为1则直接返回multipleResults的第一个元素否则返回整个multipleResults
  }

  @Override
  public <E> Cursor<E> handleCursorResultSets(Statement stmt) throws SQLException {
    ErrorContext.instance().activity("handling cursor results").object(mappedStatement.getId());

    ResultSetWrapper rsw = getFirstResultSet(stmt);

    List<ResultMap> resultMaps = mappedStatement.getResultMaps();

    int resultMapCount = resultMaps.size();
    validateResultMapsCount(rsw, resultMapCount);
    if (resultMapCount != 1) {
      throw new ExecutorException("Cursor results cannot be mapped to multiple resultMaps");
    }

    ResultMap resultMap = resultMaps.get(0);
    return new DefaultCursor<>(this, resultMap, rsw, rowBounds);
  }

  private ResultSetWrapper getFirstResultSet(Statement stmt) throws SQLException {
    ResultSet rs = stmt.getResultSet();//获取结果集
    while (rs == null) {

      // 如果驱动程序没有将结果集作为第一个结果返回（HSQLDB 2.1），则继续获取第一个结果集
      if (stmt.getMoreResults()) {
        rs = stmt.getResultSet();
      } else {
        if (stmt.getUpdateCount() == -1) {
          // 没有更多的结果。必须是没有结果集
          break;
        }
      }
    }
    return rs != null ? new ResultSetWrapper(rs, configuration) : null;//如果结果集不为空则使用ResultSetWrapper包装结果集
  }

  private ResultSetWrapper getNextResultSet(Statement stmt) {
    // Making this method tolerant of bad JDBC drivers
    try {
      if (stmt.getConnection().getMetaData().supportsMultipleResultSets()) {
        // Crazy Standard JDBC way of determining if there are more results
        if (!(!stmt.getMoreResults() && stmt.getUpdateCount() == -1)) {
          ResultSet rs = stmt.getResultSet();
          if (rs == null) {
            return getNextResultSet(stmt);
          } else {
            return new ResultSetWrapper(rs, configuration);
          }
        }
      }
    } catch (Exception e) {
      // Intentionally ignored.
    }
    return null;
  }

  private void closeResultSet(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      // ignore
    }
  }

  private void cleanUpAfterHandlingResultSet() {
    nestedResultObjects.clear();
  }

  private void validateResultMapsCount(ResultSetWrapper rsw, int resultMapCount) {
    if (rsw != null && resultMapCount < 1) {
      throw new ExecutorException("A query was run and no Result Maps were found for the Mapped Statement '" + mappedStatement.getId()
          + "'.  It's likely that neither a Result Type nor a Result Map was specified.");
    }
  }

  private void handleResultSet(ResultSetWrapper rsw, ResultMap resultMap, List<Object> multipleResults, ResultMapping parentMapping) throws SQLException {
    try {
      if (parentMapping != null) {//有父类ResultMap的情况
        handleRowValues(rsw, resultMap, null, RowBounds.DEFAULT, parentMapping);
      } else {
        if (resultHandler == null) {
          DefaultResultHandler defaultResultHandler = new DefaultResultHandler(objectFactory);//创建DefaultResultHandler（其中使用了ObjectFactory创建了DefaultResultHandler内部成员变量list）
          handleRowValues(rsw, resultMap, defaultResultHandler, rowBounds, null);//解析返回结果并放入ResultHandler的list中
          multipleResults.add(defaultResultHandler.getResultList());//取出ResultHandler的list并添加到multipleResults中
        } else {
          handleRowValues(rsw, resultMap, resultHandler, rowBounds, null);
        }
      }
    } finally {
      // issue #228 (close resultsets)
      closeResultSet(rsw.getResultSet());//关闭结果集
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object> collapseSingleResultList(List<Object> multipleResults) {
    return multipleResults.size() == 1 ? (List<Object>) multipleResults.get(0) : multipleResults;
  }

  //
  // HANDLE ROWS FOR SIMPLE RESULTMAP
  //

  public void handleRowValues(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
    if (resultMap.hasNestedResultMaps()) {//是否有嵌套的结果映射
      ensureNoRowBounds();//检查分页是否合法
      checkResultHandler();//确保ResultHandler不为空且没有指定ResultOrdered且开启了SafeResultHandler
      handleRowValuesForNestedResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);
    } else {
      handleRowValuesForSimpleResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);//将ResultSet中的内容转化为ResultMap指定的对象并存储在ResultHandler的list中
    }
  }

  private void ensureNoRowBounds() {//确保返回的结果集的limits在限制内（大于0或者小于Integer的最大值，这两个界限可能会发生变化）且启用了安全行边界
    if (configuration.isSafeRowBoundsEnabled() && rowBounds != null && (rowBounds.getLimit() < RowBounds.NO_ROW_LIMIT || rowBounds.getOffset() > RowBounds.NO_ROW_OFFSET)) {
      throw new ExecutorException("Mapped Statements with nested result mappings cannot be safely constrained by RowBounds. "
          + "Use safeRowBoundsEnabled=false setting to bypass this check.");
    }
  }

  protected void checkResultHandler() {
    if (resultHandler != null && configuration.isSafeResultHandlerEnabled() && !mappedStatement.isResultOrdered()) {
      throw new ExecutorException("Mapped Statements with nested result mappings cannot be safely used with a custom ResultHandler. "
          + "Use safeResultHandlerEnabled=false setting to bypass this check "
          + "or ensure your statement returns ordered data and set resultOrdered=true on it.");
    }
  }

  private void handleRowValuesForSimpleResultMap(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping)
      throws SQLException {
    DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
    ResultSet resultSet = rsw.getResultSet();
    skipRows(resultSet, rowBounds);//根据分页情况跳过某些记录
    while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {//shouldProcessMoreRows：确保记录条数小于指定条数
      ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);//从Configuration中获取鉴别器对象
      Object rowValue = getRowValue(rsw, discriminatedResultMap, null);//将ResultSet包装为对象
      storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);//将该条返回结果存入resultContext和resultHandler中
    }
  }

  private void storeObject(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue, ResultMapping parentMapping, ResultSet rs) throws SQLException {
    if (parentMapping != null) {//如果含有父类ResultMap
      linkToParents(rs, parentMapping, rowValue);
    } else {
      callResultHandler(resultHandler, resultContext, rowValue);//将结果存放到resultContext和resultHandler中
    }
  }

  @SuppressWarnings("unchecked" /* because ResultHandler<?> is always ResultHandler<Object>*/)
  private void callResultHandler(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue) {
    resultContext.nextResultObject(rowValue);//resultContext中存储返回结果和返回结果条数，这一步是增加返回结果条数并为返回结果（resultContext中的Object）赋值
    ((ResultHandler<Object>) resultHandler).handleResult(resultContext);//resultHandler中含有一个List<Object>用于存储所有返回结果，一条结果一个Obj，多条结果用List存储
  }

  private boolean shouldProcessMoreRows(ResultContext<?> context, RowBounds rowBounds) {
    return !context.isStopped() && context.getResultCount() < rowBounds.getLimit();
  }

  private void skipRows(ResultSet rs, RowBounds rowBounds) throws SQLException {
    if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
      if (rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
        rs.absolute(rowBounds.getOffset());
      }
    } else {
      for (int i = 0; i < rowBounds.getOffset(); i++) {
        if (!rs.next()) {
          break;
        }
      }
    }
  }

  //
  // GET VALUE FROM ROW FOR SIMPLE RESULT MAP
  //

  private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix) throws SQLException {
    final ResultLoaderMap lazyLoader = new ResultLoaderMap();//创建懒加载ResultMap
    Object rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);//创建ResultMap所指定的需要封装的对象（如果采用无参构造则是空模板，没有数据）
    if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {//如果不含有能够处理resultMap指定封装对象的Handler
      final MetaObject metaObject = configuration.newMetaObject(rowValue);//使用MetaObject封装rowValue
      boolean foundValues = this.useConstructorMappings;//是否使用有参构造方法创建对象
      if (shouldApplyAutomaticMappings(resultMap, false)) {//是否采用了自动映射
        foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
      }
      foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;
      foundValues = lazyLoader.size() > 0 || foundValues;
      rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
    }
    return rowValue;
  }

  private boolean shouldApplyAutomaticMappings(ResultMap resultMap, boolean isNested) {
    if (resultMap.getAutoMapping() != null) {
      return resultMap.getAutoMapping();//是否采用自动映射
    } else {
      if (isNested) {//是否嵌套
        return AutoMappingBehavior.FULL == configuration.getAutoMappingBehavior();//检查configuration的自动映射行为是FULL
      } else {
        return AutoMappingBehavior.NONE != configuration.getAutoMappingBehavior();//检查configuration的自动映射行为不是NONE
      }
    }
  }

  //
  // PROPERTY MAPPINGS
  //

  private boolean applyPropertyMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    final List<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);//获取ResultSet中拼接后的大写的列名
    boolean foundValues = false;
    final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();//获取映射集合
    for (ResultMapping propertyMapping : propertyMappings) {
      String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);//拼接列名（ResultMap中）
      if (propertyMapping.getNestedResultMapId() != null) {
        // the user added a column attribute to a nested result map, ignore it
        column = null;
      }
      if (propertyMapping.isCompositeResult()//如果是复合结果（一个ResultMapping中含有多个ResultMapping）
          || (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH)))//列名不为空且ResultSet中含有ResultMap的列名
          || propertyMapping.getResultSet() != null) {//ResultMap中还没有存储ResultSet
        Object value = getPropertyMappingValue(rsw.getResultSet(), metaObject, propertyMapping, lazyLoader, columnPrefix);//获取该列的值
        // issue #541 make property optional
        final String property = propertyMapping.getProperty();//获取结果类中该成员的名称
        if (property == null) {
          continue;
        } else if (value == DEFERRED) {
          foundValues = true;
          continue;
        }
        if (value != null) {
          foundValues = true;
        }
        if (value != null || (configuration.isCallSettersOnNulls() && !metaObject.getSetterType(property).isPrimitive())) {
          // gcode issue #377, call setter on nulls (value is not 'found')
          metaObject.setValue(property, value);//执行set方法注入
        }
      }
    }
    return foundValues;
  }

  private Object getPropertyMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    if (propertyMapping.getNestedQueryId() != null) {//含有嵌套查询
      return getNestedQueryMappingValue(rs, metaResultObject, propertyMapping, lazyLoader, columnPrefix);
    } else if (propertyMapping.getResultSet() != null) {//如果ResultMapping中已经含有ResultSet（代表这里一个复合型的ResultMapping，其下的ResultSet已经完成）
      addPendingChildRelation(rs, metaResultObject, propertyMapping);   // TODO is that OK?
      return DEFERRED;
    } else {
      final TypeHandler<?> typeHandler = propertyMapping.getTypeHandler();//获取该ResultMapping的typeHandler
      final String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);//拼接列名
      return typeHandler.getResult(rs, column);//获取该列的值
    }
  }

  private List<UnMappedColumnAutoMapping> createAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
    final String mapKey = resultMap.getId() + ":" + columnPrefix;//拼接Map的key：resultMap的ID+列前缀
    List<UnMappedColumnAutoMapping> autoMapping = autoMappingsCache.get(mapKey);//查看缓存中是否存在该mapkey对应的自动映射Map
    if (autoMapping == null) {//缓存中没有的情况下
      autoMapping = new ArrayList<>();//用于存储未映射列名的集合
      final List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);//获取未映射的列名集合（通过MapKey）
      for (String columnName : unmappedColumnNames) {//对于未映射列名的集合的处理
        String propertyName = columnName;
        if (columnPrefix != null && !columnPrefix.isEmpty()) {//含有列名前缀的前提下
          //
          // 指定 columnPrefix 时，忽略不带前缀的列
          if (columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {//如果作为列前缀开头
            propertyName = columnName.substring(columnPrefix.length());//去除列前缀
          } else {
            continue;
          }
        }
        final String property = metaObject.findProperty(propertyName, configuration.isMapUnderscoreToCamelCase());//从resultMap找到该值
        if (property != null && metaObject.hasSetter(property)) {//如果在metaObject中找到了该成员变量并且该成员变量含有set方法则
          if (resultMap.getMappedProperties().contains(property)) {//排除mappedProperties中的属性
            continue;
          }
          final Class<?> propertyType = metaObject.getSetterType(property);//获取set方法参数的类型
          if (typeHandlerRegistry.hasTypeHandler(propertyType, rsw.getJdbcType(columnName))) {//查看是否含有set方法参数类型的TypeHandler
            final TypeHandler<?> typeHandler = rsw.getTypeHandler(propertyType, columnName);
            autoMapping.add(new UnMappedColumnAutoMapping(columnName, property, typeHandler, propertyType.isPrimitive()));//向自动结果映射中添加该记录
          } else {
            configuration.getAutoMappingUnknownColumnBehavior()
                .doAction(mappedStatement, columnName, property, propertyType);//记录日志
          }
        } else {
          configuration.getAutoMappingUnknownColumnBehavior()
              .doAction(mappedStatement, columnName, (property != null) ? property : propertyName, null);
        }
      }
      autoMappingsCache.put(mapKey, autoMapping);//将该自动映射Mapping放入缓存中
    }
    return autoMapping;
  }

  private boolean applyAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
    List<UnMappedColumnAutoMapping> autoMapping = createAutomaticMappings(rsw, resultMap, metaObject, columnPrefix);//创建一个空的自动映射Mapping
    boolean foundValues = false;
    if (!autoMapping.isEmpty()) {
      for (UnMappedColumnAutoMapping mapping : autoMapping) {
        final Object value = mapping.typeHandler.getResult(rsw.getResultSet(), mapping.column);//获取该列的值
        if (value != null) {
          foundValues = true;
        }
        if (value != null || (configuration.isCallSettersOnNulls() && !mapping.primitive)) {
          // gcode issue #377, call setter on nulls (value is not 'found')
          metaObject.setValue(mapping.property, value);//设置值
        }
      }
    }
    return foundValues;
  }

  // MULTIPLE RESULT SETS

  private void linkToParents(ResultSet rs, ResultMapping parentMapping, Object rowValue) throws SQLException {
    CacheKey parentKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(), parentMapping.getForeignColumn());
    List<PendingRelation> parents = pendingRelations.get(parentKey);
    if (parents != null) {
      for (PendingRelation parent : parents) {
        if (parent != null && rowValue != null) {
          linkObjects(parent.metaObject, parent.propertyMapping, rowValue);
        }
      }
    }
  }

  private void addPendingChildRelation(ResultSet rs, MetaObject metaResultObject, ResultMapping parentMapping) throws SQLException {
    CacheKey cacheKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(), parentMapping.getColumn());
    PendingRelation deferLoad = new PendingRelation();
    deferLoad.metaObject = metaResultObject;
    deferLoad.propertyMapping = parentMapping;
    List<PendingRelation> relations = pendingRelations.computeIfAbsent(cacheKey, k -> new ArrayList<>());
    // issue #255
    relations.add(deferLoad);
    ResultMapping previous = nextResultMaps.get(parentMapping.getResultSet());
    if (previous == null) {
      nextResultMaps.put(parentMapping.getResultSet(), parentMapping);
    } else {
      if (!previous.equals(parentMapping)) {
        throw new ExecutorException("Two different properties are mapped to the same resultSet");
      }
    }
  }

  private CacheKey createKeyForMultipleResults(ResultSet rs, ResultMapping resultMapping, String names, String columns) throws SQLException {
    CacheKey cacheKey = new CacheKey();
    cacheKey.update(resultMapping);
    if (columns != null && names != null) {
      String[] columnsArray = columns.split(",");
      String[] namesArray = names.split(",");
      for (int i = 0; i < columnsArray.length; i++) {
        Object value = rs.getString(columnsArray[i]);
        if (value != null) {
          cacheKey.update(namesArray[i]);
          cacheKey.update(value);
        }
      }
    }
    return cacheKey;
  }

  //
  // INSTANTIATION & CONSTRUCTOR MAPPING
  //

  private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, ResultLoaderMap lazyLoader, String columnPrefix) throws SQLException {
    this.useConstructorMappings = false; // 重置之前的映射结果
    final List<Class<?>> constructorArgTypes = new ArrayList<>();//创建构造方法参数类型list
    final List<Object> constructorArgs = new ArrayList<>();//创建构造方法参数值list
    Object resultObject = createResultObject(rsw, resultMap, constructorArgTypes, constructorArgs, columnPrefix);//创建结果对象（没有任何数据只是个空模板）
    if (resultObject != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {//如果不含有结果对象类型的Handler
      final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();//获取ResultMap
      for (ResultMapping propertyMapping : propertyMappings) {
        // issue gcode #109 && issue #149
        if (propertyMapping.getNestedQueryId() != null && propertyMapping.isLazy()) {//检查ResultMap中是否存在嵌套查询且采用懒加载如果有则通过configuration创建代理对象（使用Javassist方式）作为resultObject
          resultObject = configuration.getProxyFactory().createProxy(resultObject, lazyLoader, configuration, objectFactory, constructorArgTypes, constructorArgs);
          break;
        }
      }
    }
    this.useConstructorMappings = resultObject != null && !constructorArgTypes.isEmpty(); // set current mapping result
    return resultObject;
  }

  private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix)
      throws SQLException {
    final Class<?> resultType = resultMap.getType();//获取resultMap需要包装的对象类型
    final MetaClass metaType = MetaClass.forClass(resultType, reflectorFactory);//resultType封装为MetaClass
    final List<ResultMapping> constructorMappings = resultMap.getConstructorResultMappings();//获取构造方法Mapping
    if (hasTypeHandlerForResultObject(rsw, resultType)) {//如果含有能够处理结果类型的Handler
      return createPrimitiveResultObject(rsw, resultMap, columnPrefix);//创建原始结果对象
    } else if (!constructorMappings.isEmpty()) {//如果含有构造方法mapping
      return createParameterizedResultObject(rsw, resultType, constructorMappings, constructorArgTypes, constructorArgs, columnPrefix);
    } else if (resultType.isInterface() || metaType.hasDefaultConstructor()) {//如果resultType是接口并且含有默认的构造方法
      return objectFactory.create(resultType);//采用objectFactory创建
    } else if (shouldApplyAutomaticMappings(resultMap, false)) {
      return createByConstructorSignature(rsw, resultType, constructorArgTypes, constructorArgs);
    }
    throw new ExecutorException("Do not know how to create an instance of " + resultType);
  }

  Object createParameterizedResultObject(ResultSetWrapper rsw, Class<?> resultType, List<ResultMapping> constructorMappings,
                                         List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix) {
    boolean foundValues = false;
    for (ResultMapping constructorMapping : constructorMappings) {
      final Class<?> parameterType = constructorMapping.getJavaType();
      final String column = constructorMapping.getColumn();
      final Object value;
      try {
        if (constructorMapping.getNestedQueryId() != null) {
          value = getNestedQueryConstructorValue(rsw.getResultSet(), constructorMapping, columnPrefix);
        } else if (constructorMapping.getNestedResultMapId() != null) {
          final ResultMap resultMap = configuration.getResultMap(constructorMapping.getNestedResultMapId());
          value = getRowValue(rsw, resultMap, getColumnPrefix(columnPrefix, constructorMapping));
        } else {
          final TypeHandler<?> typeHandler = constructorMapping.getTypeHandler();
          value = typeHandler.getResult(rsw.getResultSet(), prependPrefix(column, columnPrefix));
        }
      } catch (ResultMapException | SQLException e) {
        throw new ExecutorException("Could not process result for mapping: " + constructorMapping, e);
      }
      constructorArgTypes.add(parameterType);
      constructorArgs.add(value);
      foundValues = value != null || foundValues;
    }
    return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
  }

  private Object createByConstructorSignature(ResultSetWrapper rsw, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs) throws SQLException {
    final Constructor<?>[] constructors = resultType.getDeclaredConstructors();
    final Constructor<?> defaultConstructor = findDefaultConstructor(constructors);
    if (defaultConstructor != null) {
      return createUsingConstructor(rsw, resultType, constructorArgTypes, constructorArgs, defaultConstructor);
    } else {
      for (Constructor<?> constructor : constructors) {
        if (allowedConstructorUsingTypeHandlers(constructor, rsw.getJdbcTypes())) {
          return createUsingConstructor(rsw, resultType, constructorArgTypes, constructorArgs, constructor);
        }
      }
    }
    throw new ExecutorException("No constructor found in " + resultType.getName() + " matching " + rsw.getClassNames());
  }

  private Object createUsingConstructor(ResultSetWrapper rsw, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, Constructor<?> constructor) throws SQLException {
    boolean foundValues = false;
    for (int i = 0; i < constructor.getParameterTypes().length; i++) {
      Class<?> parameterType = constructor.getParameterTypes()[i];
      String columnName = rsw.getColumnNames().get(i);
      TypeHandler<?> typeHandler = rsw.getTypeHandler(parameterType, columnName);
      Object value = typeHandler.getResult(rsw.getResultSet(), columnName);
      constructorArgTypes.add(parameterType);
      constructorArgs.add(value);
      foundValues = value != null || foundValues;
    }
    return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
  }

  private Constructor<?> findDefaultConstructor(final Constructor<?>[] constructors) {
    if (constructors.length == 1) {
      return constructors[0];
    }

    for (final Constructor<?> constructor : constructors) {
      if (constructor.isAnnotationPresent(AutomapConstructor.class)) {
        return constructor;
      }
    }
    return null;
  }

  private boolean allowedConstructorUsingTypeHandlers(final Constructor<?> constructor, final List<JdbcType> jdbcTypes) {
    final Class<?>[] parameterTypes = constructor.getParameterTypes();
    if (parameterTypes.length != jdbcTypes.size()) {
      return false;
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      if (!typeHandlerRegistry.hasTypeHandler(parameterTypes[i], jdbcTypes.get(i))) {
        return false;
      }
    }
    return true;
  }

  private Object createPrimitiveResultObject(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix) throws SQLException {
    final Class<?> resultType = resultMap.getType();
    final String columnName;
    if (!resultMap.getResultMappings().isEmpty()) {//如果resultMap不为空则
      final List<ResultMapping> resultMappingList = resultMap.getResultMappings();
      final ResultMapping mapping = resultMappingList.get(0);//获取第一个resultMapping
      columnName = prependPrefix(mapping.getColumn(), columnPrefix);//拼接行名称
    } else {
      columnName = rsw.getColumnNames().get(0);
    }
    final TypeHandler<?> typeHandler = rsw.getTypeHandler(resultType, columnName);
    return typeHandler.getResult(rsw.getResultSet(), columnName);
  }

  //
  // NESTED QUERY
  //

  private Object getNestedQueryConstructorValue(ResultSet rs, ResultMapping constructorMapping, String columnPrefix) throws SQLException {
    final String nestedQueryId = constructorMapping.getNestedQueryId();
    final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
    final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
    final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, constructorMapping, nestedQueryParameterType, columnPrefix);
    Object value = null;
    if (nestedQueryParameterObject != null) {
      final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
      final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT, nestedBoundSql);
      final Class<?> targetType = constructorMapping.getJavaType();
      final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery, nestedQueryParameterObject, targetType, key, nestedBoundSql);
      value = resultLoader.loadResult();
    }
    return value;
  }

  private Object getNestedQueryMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    final String nestedQueryId = propertyMapping.getNestedQueryId();
    final String property = propertyMapping.getProperty();
    final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
    final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
    final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, propertyMapping, nestedQueryParameterType, columnPrefix);
    Object value = null;
    if (nestedQueryParameterObject != null) {
      final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
      final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT, nestedBoundSql);
      final Class<?> targetType = propertyMapping.getJavaType();
      if (executor.isCached(nestedQuery, key)) {
        executor.deferLoad(nestedQuery, metaResultObject, property, key, targetType);
        value = DEFERRED;
      } else {
        final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery, nestedQueryParameterObject, targetType, key, nestedBoundSql);
        if (propertyMapping.isLazy()) {
          lazyLoader.addLoader(property, metaResultObject, resultLoader);
          value = DEFERRED;
        } else {
          value = resultLoader.loadResult();
        }
      }
    }
    return value;
  }

  private Object prepareParameterForNestedQuery(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    if (resultMapping.isCompositeResult()) {
      return prepareCompositeKeyParameter(rs, resultMapping, parameterType, columnPrefix);
    } else {
      return prepareSimpleKeyParameter(rs, resultMapping, parameterType, columnPrefix);
    }
  }

  private Object prepareSimpleKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    final TypeHandler<?> typeHandler;
    if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
      typeHandler = typeHandlerRegistry.getTypeHandler(parameterType);
    } else {
      typeHandler = typeHandlerRegistry.getUnknownTypeHandler();
    }
    return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
  }

  private Object prepareCompositeKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    final Object parameterObject = instantiateParameterObject(parameterType);
    final MetaObject metaObject = configuration.newMetaObject(parameterObject);
    boolean foundValues = false;
    for (ResultMapping innerResultMapping : resultMapping.getComposites()) {
      final Class<?> propType = metaObject.getSetterType(innerResultMapping.getProperty());
      final TypeHandler<?> typeHandler = typeHandlerRegistry.getTypeHandler(propType);
      final Object propValue = typeHandler.getResult(rs, prependPrefix(innerResultMapping.getColumn(), columnPrefix));
      // issue #353 & #560 do not execute nested query if key is null
      if (propValue != null) {
        metaObject.setValue(innerResultMapping.getProperty(), propValue);
        foundValues = true;
      }
    }
    return foundValues ? parameterObject : null;
  }

  private Object instantiateParameterObject(Class<?> parameterType) {
    if (parameterType == null) {
      return new HashMap<>();
    } else if (ParamMap.class.equals(parameterType)) {
      return new HashMap<>(); // issue #649
    } else {
      return objectFactory.create(parameterType);
    }
  }

  //
  // DISCRIMINATOR
  //

  public ResultMap resolveDiscriminatedResultMap(ResultSet rs, ResultMap resultMap, String columnPrefix) throws SQLException {
    Set<String> pastDiscriminators = new HashSet<>();
    Discriminator discriminator = resultMap.getDiscriminator();
    while (discriminator != null) {
      final Object value = getDiscriminatorValue(rs, discriminator, columnPrefix);
      final String discriminatedMapId = discriminator.getMapIdFor(String.valueOf(value));
      if (configuration.hasResultMap(discriminatedMapId)) {
        resultMap = configuration.getResultMap(discriminatedMapId);
        Discriminator lastDiscriminator = discriminator;
        discriminator = resultMap.getDiscriminator();
        if (discriminator == lastDiscriminator || !pastDiscriminators.add(discriminatedMapId)) {
          break;
        }
      } else {
        break;
      }
    }
    return resultMap;
  }

  private Object getDiscriminatorValue(ResultSet rs, Discriminator discriminator, String columnPrefix) throws SQLException {
    final ResultMapping resultMapping = discriminator.getResultMapping();
    final TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
    return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
  }

  private String prependPrefix(String columnName, String prefix) {
    if (columnName == null || columnName.length() == 0 || prefix == null || prefix.length() == 0) {
      return columnName;
    }
    return prefix + columnName;
  }

  //
  // HANDLE NESTED RESULT MAPS
  //

  private void handleRowValuesForNestedResultMap(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
    final DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
    ResultSet resultSet = rsw.getResultSet();
    skipRows(resultSet, rowBounds);
    Object rowValue = previousRowValue;
    while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {
      final ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);
      final CacheKey rowKey = createRowKey(discriminatedResultMap, rsw, null);
      Object partialObject = nestedResultObjects.get(rowKey);
      // issue #577 && #542
      if (mappedStatement.isResultOrdered()) {
        if (partialObject == null && rowValue != null) {
          nestedResultObjects.clear();
          storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
        }
        rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
      } else {
        rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
        if (partialObject == null) {
          storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
        }
      }
    }
    if (rowValue != null && mappedStatement.isResultOrdered() && shouldProcessMoreRows(resultContext, rowBounds)) {
      storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
      previousRowValue = null;
    } else if (rowValue != null) {
      previousRowValue = rowValue;
    }
  }

  //
  // GET VALUE FROM ROW FOR NESTED RESULT MAP
  //

  private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, CacheKey combinedKey, String columnPrefix, Object partialObject) throws SQLException {
    final String resultMapId = resultMap.getId();
    Object rowValue = partialObject;
    if (rowValue != null) {
      final MetaObject metaObject = configuration.newMetaObject(rowValue);
      putAncestor(rowValue, resultMapId);
      applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, false);
      ancestorObjects.remove(resultMapId);
    } else {
      final ResultLoaderMap lazyLoader = new ResultLoaderMap();
      rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);
      if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
        final MetaObject metaObject = configuration.newMetaObject(rowValue);
        boolean foundValues = this.useConstructorMappings;
        if (shouldApplyAutomaticMappings(resultMap, true)) {
          foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
        }
        foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;
        putAncestor(rowValue, resultMapId);
        foundValues = applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, true) || foundValues;
        ancestorObjects.remove(resultMapId);
        foundValues = lazyLoader.size() > 0 || foundValues;
        rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
      }
      if (combinedKey != CacheKey.NULL_CACHE_KEY) {
        nestedResultObjects.put(combinedKey, rowValue);
      }
    }
    return rowValue;
  }

  private void putAncestor(Object resultObject, String resultMapId) {
    ancestorObjects.put(resultMapId, resultObject);
  }

  //
  // NESTED RESULT MAP (JOIN MAPPING)
  //

  private boolean applyNestedResultMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String parentPrefix, CacheKey parentRowKey, boolean newObject) {
    boolean foundValues = false;
    for (ResultMapping resultMapping : resultMap.getPropertyResultMappings()) {
      final String nestedResultMapId = resultMapping.getNestedResultMapId();
      if (nestedResultMapId != null && resultMapping.getResultSet() == null) {
        try {
          final String columnPrefix = getColumnPrefix(parentPrefix, resultMapping);
          final ResultMap nestedResultMap = getNestedResultMap(rsw.getResultSet(), nestedResultMapId, columnPrefix);
          if (resultMapping.getColumnPrefix() == null) {
            // try to fill circular reference only when columnPrefix
            // is not specified for the nested result map (issue #215)
            Object ancestorObject = ancestorObjects.get(nestedResultMapId);
            if (ancestorObject != null) {
              if (newObject) {
                linkObjects(metaObject, resultMapping, ancestorObject); // issue #385
              }
              continue;
            }
          }
          final CacheKey rowKey = createRowKey(nestedResultMap, rsw, columnPrefix);
          final CacheKey combinedKey = combineKeys(rowKey, parentRowKey);
          Object rowValue = nestedResultObjects.get(combinedKey);
          boolean knownValue = rowValue != null;
          instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject); // mandatory
          if (anyNotNullColumnHasValue(resultMapping, columnPrefix, rsw)) {
            rowValue = getRowValue(rsw, nestedResultMap, combinedKey, columnPrefix, rowValue);
            if (rowValue != null && !knownValue) {
              linkObjects(metaObject, resultMapping, rowValue);
              foundValues = true;
            }
          }
        } catch (SQLException e) {
          throw new ExecutorException("Error getting nested result map values for '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
        }
      }
    }
    return foundValues;
  }

  private String getColumnPrefix(String parentPrefix, ResultMapping resultMapping) {
    final StringBuilder columnPrefixBuilder = new StringBuilder();
    if (parentPrefix != null) {
      columnPrefixBuilder.append(parentPrefix);
    }
    if (resultMapping.getColumnPrefix() != null) {
      columnPrefixBuilder.append(resultMapping.getColumnPrefix());
    }
    return columnPrefixBuilder.length() == 0 ? null : columnPrefixBuilder.toString().toUpperCase(Locale.ENGLISH);
  }

  private boolean anyNotNullColumnHasValue(ResultMapping resultMapping, String columnPrefix, ResultSetWrapper rsw) throws SQLException {
    Set<String> notNullColumns = resultMapping.getNotNullColumns();
    if (notNullColumns != null && !notNullColumns.isEmpty()) {
      ResultSet rs = rsw.getResultSet();
      for (String column : notNullColumns) {
        rs.getObject(prependPrefix(column, columnPrefix));
        if (!rs.wasNull()) {
          return true;
        }
      }
      return false;
    } else if (columnPrefix != null) {
      for (String columnName : rsw.getColumnNames()) {
        if (columnName.toUpperCase().startsWith(columnPrefix.toUpperCase())) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  private ResultMap getNestedResultMap(ResultSet rs, String nestedResultMapId, String columnPrefix) throws SQLException {
    ResultMap nestedResultMap = configuration.getResultMap(nestedResultMapId);
    return resolveDiscriminatedResultMap(rs, nestedResultMap, columnPrefix);
  }

  //
  // UNIQUE RESULT KEY
  //

  private CacheKey createRowKey(ResultMap resultMap, ResultSetWrapper rsw, String columnPrefix) throws SQLException {
    final CacheKey cacheKey = new CacheKey();
    cacheKey.update(resultMap.getId());
    List<ResultMapping> resultMappings = getResultMappingsForRowKey(resultMap);
    if (resultMappings.isEmpty()) {
      if (Map.class.isAssignableFrom(resultMap.getType())) {
        createRowKeyForMap(rsw, cacheKey);
      } else {
        createRowKeyForUnmappedProperties(resultMap, rsw, cacheKey, columnPrefix);
      }
    } else {
      createRowKeyForMappedProperties(resultMap, rsw, cacheKey, resultMappings, columnPrefix);
    }
    if (cacheKey.getUpdateCount() < 2) {
      return CacheKey.NULL_CACHE_KEY;
    }
    return cacheKey;
  }

  private CacheKey combineKeys(CacheKey rowKey, CacheKey parentRowKey) {
    if (rowKey.getUpdateCount() > 1 && parentRowKey.getUpdateCount() > 1) {
      CacheKey combinedKey;
      try {
        combinedKey = rowKey.clone();
      } catch (CloneNotSupportedException e) {
        throw new ExecutorException("Error cloning cache key.  Cause: " + e, e);
      }
      combinedKey.update(parentRowKey);
      return combinedKey;
    }
    return CacheKey.NULL_CACHE_KEY;
  }

  private List<ResultMapping> getResultMappingsForRowKey(ResultMap resultMap) {
    List<ResultMapping> resultMappings = resultMap.getIdResultMappings();
    if (resultMappings.isEmpty()) {
      resultMappings = resultMap.getPropertyResultMappings();
    }
    return resultMappings;
  }

  private void createRowKeyForMappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey, List<ResultMapping> resultMappings, String columnPrefix) throws SQLException {
    for (ResultMapping resultMapping : resultMappings) {
      if (resultMapping.getNestedResultMapId() != null && resultMapping.getResultSet() == null) {
        // Issue #392
        final ResultMap nestedResultMap = configuration.getResultMap(resultMapping.getNestedResultMapId());
        createRowKeyForMappedProperties(nestedResultMap, rsw, cacheKey, nestedResultMap.getConstructorResultMappings(),
            prependPrefix(resultMapping.getColumnPrefix(), columnPrefix));
      } else if (resultMapping.getNestedQueryId() == null) {
        final String column = prependPrefix(resultMapping.getColumn(), columnPrefix);
        final TypeHandler<?> th = resultMapping.getTypeHandler();
        List<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);
        // Issue #114
        if (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH))) {
          final Object value = th.getResult(rsw.getResultSet(), column);
          if (value != null || configuration.isReturnInstanceForEmptyRow()) {
            cacheKey.update(column);
            cacheKey.update(value);
          }
        }
      }
    }
  }

  private void createRowKeyForUnmappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey, String columnPrefix) throws SQLException {
    final MetaClass metaType = MetaClass.forClass(resultMap.getType(), reflectorFactory);
    List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);
    for (String column : unmappedColumnNames) {
      String property = column;
      if (columnPrefix != null && !columnPrefix.isEmpty()) {
        // When columnPrefix is specified, ignore columns without the prefix.
        if (column.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
          property = column.substring(columnPrefix.length());
        } else {
          continue;
        }
      }
      if (metaType.findProperty(property, configuration.isMapUnderscoreToCamelCase()) != null) {
        String value = rsw.getResultSet().getString(column);
        if (value != null) {
          cacheKey.update(column);
          cacheKey.update(value);
        }
      }
    }
  }

  private void createRowKeyForMap(ResultSetWrapper rsw, CacheKey cacheKey) throws SQLException {
    List<String> columnNames = rsw.getColumnNames();
    for (String columnName : columnNames) {
      final String value = rsw.getResultSet().getString(columnName);
      if (value != null) {
        cacheKey.update(columnName);
        cacheKey.update(value);
      }
    }
  }

  private void linkObjects(MetaObject metaObject, ResultMapping resultMapping, Object rowValue) {
    final Object collectionProperty = instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject);
    if (collectionProperty != null) {
      final MetaObject targetMetaObject = configuration.newMetaObject(collectionProperty);
      targetMetaObject.add(rowValue);
    } else {
      metaObject.setValue(resultMapping.getProperty(), rowValue);
    }
  }

  private Object instantiateCollectionPropertyIfAppropriate(ResultMapping resultMapping, MetaObject metaObject) {
    final String propertyName = resultMapping.getProperty();
    Object propertyValue = metaObject.getValue(propertyName);
    if (propertyValue == null) {
      Class<?> type = resultMapping.getJavaType();
      if (type == null) {
        type = metaObject.getSetterType(propertyName);
      }
      try {
        if (objectFactory.isCollection(type)) {
          propertyValue = objectFactory.create(type);
          metaObject.setValue(propertyName, propertyValue);
          return propertyValue;
        }
      } catch (Exception e) {
        throw new ExecutorException("Error instantiating collection property for result '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
      }
    } else if (objectFactory.isCollection(propertyValue.getClass())) {
      return propertyValue;
    }
    return null;
  }

  private boolean hasTypeHandlerForResultObject(ResultSetWrapper rsw, Class<?> resultType) {
    if (rsw.getColumnNames().size() == 1) {
      return typeHandlerRegistry.hasTypeHandler(resultType, rsw.getJdbcType(rsw.getColumnNames().get(0)));
    }
    return typeHandlerRegistry.hasTypeHandler(resultType);
  }

}
