/**
 *    Copyright 2009-2020 the original author or authors.
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
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

  private final XPathParser parser;
  private final MapperBuilderAssistant builderAssistant;
  private final Map<String, XNode> sqlFragments;
  private final String resource;

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(reader, configuration, resource, sqlFragments);
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()),
        configuration, resource, sqlFragments);
  }

  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(inputStream, configuration, resource, sqlFragments);
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
        configuration, resource, sqlFragments);
  }

  private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    super(configuration);
    this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
    this.parser = parser;
    this.sqlFragments = sqlFragments;
    this.resource = resource;
  }

  public void parse() {
    if (!configuration.isResourceLoaded(resource)) {
      configurationElement(parser.evalNode("/mapper"));//从Mapper标签开始解析
      configuration.addLoadedResource(resource);//configuration中的成员变量Set<String> loadedResources = new HashSet<>();用于保存已经加载的资源
      bindMapperForNamespace();//查看是否有mapper接口与该namespace绑定如果有就加载该类并将其Class放入loadedResources中
    }

    parsePendingResultMaps();//解析未能完全解析的ResultMap
    parsePendingCacheRefs();//解析未能完全解析的缓存引用
    parsePendingStatements();//解析未能完全解析的语句
  }

  public XNode getSqlFragment(String refid) {
    return sqlFragments.get(refid);
  }

    private void configurationElement(XNode context) {
    try {
      String namespace = context.getStringAttribute("namespace");
      if (namespace == null || namespace.equals("")) {//namspace不能为空
        throw new BuilderException("Mapper's namespace cannot be empty");
      }
      builderAssistant.setCurrentNamespace(namespace);//设置builderAssistant的namespace
      cacheRefElement(context.evalNode("cache-ref"));//解析缓存引用
      cacheElement(context.evalNode("cache"));//解析缓存
      parameterMapElement(context.evalNodes("/mapper/parameterMap"));//解析parameterMap（似乎已被弃用）
      resultMapElements(context.evalNodes("/mapper/resultMap"));//解析resultMap
      sqlElement(context.evalNodes("/mapper/sql"));//解析SQL标签
      buildStatementFromContext(context.evalNodes("select|insert|update|delete"));//解析Select等标签
    } catch (Exception e) {
      throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e, e);
    }
  }

  private void buildStatementFromContext(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      buildStatementFromContext(list, configuration.getDatabaseId());
    }
    buildStatementFromContext(list, null);
  }

  private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
    for (XNode context : list) {
      final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant, context, requiredDatabaseId);//创建statementParser
      try {
        statementParser.parseStatementNode();//解析SQL语句标签
      } catch (IncompleteElementException e) {
        configuration.addIncompleteStatement(statementParser);
      }
    }
  }

  private void parsePendingResultMaps() {
    Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
    synchronized (incompleteResultMaps) {
      Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().resolve();
          iter.remove();
        } catch (IncompleteElementException e) {
          // ResultMap is still missing a resource...
        }
      }
    }
  }

  private void parsePendingCacheRefs() {
    Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
    synchronized (incompleteCacheRefs) {
      Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().resolveCacheRef();
          iter.remove();
        } catch (IncompleteElementException e) {
          // Cache ref is still missing a resource...
        }
      }
    }
  }

  private void parsePendingStatements() {
    Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
    synchronized (incompleteStatements) {
      Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
      while (iter.hasNext()) {
        try {
          iter.next().parseStatementNode();
          iter.remove();
        } catch (IncompleteElementException e) {
          // Statement is still missing a resource...
        }
      }
    }
  }

  private void cacheRefElement(XNode context) {
    if (context != null) {
      configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));//添加缓存引用（放入Configuration成员变量中的protected final Map<String, String> cacheRefMap = new HashMap<>();）
      CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant, context.getStringAttribute("namespace"));//简单赋值，CacheRefResolver中包含了assistant和被引用的缓存
      try {
        cacheRefResolver.resolveCacheRef();//解析缓存引用
      } catch (IncompleteElementException e) {
        configuration.addIncompleteCacheRef(cacheRefResolver);//只要抛出IncompleteElementException异常则将该缓存引用放入incompleteCacheRefs中
      }
    }
  }

  private void cacheElement(XNode context) {
    if (context != null) {
      String type = context.getStringAttribute("type", "PERPETUAL");//针对自定义缓存（<cache type="com.domain.something.MyCustomCache"/>）
      Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);//从别名注册器中查找该类
      String eviction = context.getStringAttribute("eviction", "LRU");//获取缓存策略类型
      Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
      Long flushInterval = context.getLongAttribute("flushInterval");//刷新频率
      Integer size = context.getIntAttribute("size");//缓存大写（能存多少个对象）
      boolean readWrite = !context.getBooleanAttribute("readOnly", false);//是否只读
      boolean blocking = context.getBooleanAttribute("blocking", false);//是否堵塞
      Properties props = context.getChildrenAsProperties();//缓存标签内部配置
      builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);//创建最终Cache并设置currentCache
    }
  }

  private void parameterMapElement(List<XNode> list) {
    for (XNode parameterMapNode : list) {
      String id = parameterMapNode.getStringAttribute("id");
      String type = parameterMapNode.getStringAttribute("type");
      Class<?> parameterClass = resolveClass(type);
      List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
      List<ParameterMapping> parameterMappings = new ArrayList<>();
      for (XNode parameterNode : parameterNodes) {
        String property = parameterNode.getStringAttribute("property");
        String javaType = parameterNode.getStringAttribute("javaType");
        String jdbcType = parameterNode.getStringAttribute("jdbcType");
        String resultMap = parameterNode.getStringAttribute("resultMap");
        String mode = parameterNode.getStringAttribute("mode");
        String typeHandler = parameterNode.getStringAttribute("typeHandler");
        Integer numericScale = parameterNode.getIntAttribute("numericScale");
        ParameterMode modeEnum = resolveParameterMode(mode);
        Class<?> javaTypeClass = resolveClass(javaType);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property, javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
        parameterMappings.add(parameterMapping);
      }
      builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
    }
  }

  private void resultMapElements(List<XNode> list) {
    for (XNode resultMapNode : list) {//遍历该Mapper文件中所有ResultMap
      try {
        resultMapElement(resultMapNode);
      } catch (IncompleteElementException e) {
        // ignore, it will be retried
      }
    }
  }

  private ResultMap resultMapElement(XNode resultMapNode) {
    return resultMapElement(resultMapNode, Collections.emptyList(), null);
  }

  private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings, Class<?> enclosingType) {
    ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());
    String type = resultMapNode.getStringAttribute("type",
        resultMapNode.getStringAttribute("ofType",
            resultMapNode.getStringAttribute("resultType",
                resultMapNode.getStringAttribute("javaType"))));//优先级type>ofType>resultType>javaType
    Class<?> typeClass = resolveClass(type);//在别名注册器中查找该type
    if (typeClass == null) {//如果不含有以上这些type属性
      typeClass = inheritEnclosingType(resultMapNode, enclosingType);//说明该标签可能是association或者case，对于association标签可以通过其父级的ResultMap和association所对应的属性名称获取到set方法上的参数类型（也即是association所对应的类型）
    }
    Discriminator discriminator = null;//鉴别器
    List<ResultMapping> resultMappings = new ArrayList<>(additionalResultMappings);//additionalResultMappings是需要额外添加的Mapping
    List<XNode> resultChildren = resultMapNode.getChildren();//获取当前标签下的子标签
    for (XNode resultChild : resultChildren) {//遍历ResultMap下的所有子标签
      if ("constructor".equals(resultChild.getName())) {//构造标签
        processConstructorElement(resultChild, typeClass, resultMappings);//解析constructor标签
      } else if ("discriminator".equals(resultChild.getName())) {//鉴别器标签
        discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);//解析鉴别器标签
      } else {
        List<ResultFlag> flags = new ArrayList<>();
        if ("id".equals(resultChild.getName())) {
          flags.add(ResultFlag.ID);//如果是主键映射则添加ID 的flag
        }
        resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));//向resultMappings中添加该标签
      }
    }
    String id = resultMapNode.getStringAttribute("id",
            resultMapNode.getValueBasedIdentifier());
    String extend = resultMapNode.getStringAttribute("extends");//获取父类ResultMap
    Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");//是否开启自动映射
    ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend, discriminator, resultMappings, autoMapping);
    try {
      return resultMapResolver.resolve();//解析整个ResultMap
    } catch (IncompleteElementException  e) {
      configuration.addIncompleteResultMap(resultMapResolver);
      throw e;
    }
  }

  protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {//enclosingType：该标签上级的标签的父类（假设ResultMap中含有一个association标签，该标签没有提供type属性，而ResultMap提供了type属性则enclosingType就是这个类型）
    if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {//如果该节点是association标签且该节点没有resultMap属性
      String property = resultMapNode.getStringAttribute("property");//获取property属性值（获取association在最后的结果类中的成员变量名称）
      if (property != null && enclosingType != null) {
        MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());//包装enclosingType
        return metaResultType.getSetterType(property);//通过enclosingType的set方法和属性名称获取到set方法上的参数类型（也就是association应该对应的返回类型）
      }
    } else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {//如果是case标签则之间返回父类Class
      return enclosingType;
    }
    return null;
  }

  private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings) {
    List<XNode> argChildren = resultChild.getChildren();
    for (XNode argChild : argChildren) {//获取构造方法标签下的子标签（一共有Idarg和arg两种）
      List<ResultFlag> flags = new ArrayList<>();//枚举类型，有CONSTRUCTOR和ID两种
      flags.add(ResultFlag.CONSTRUCTOR);//用作标记，告诉下面的方法目前处理的标签来自构造方法标签
      if ("idArg".equals(argChild.getName())) {
        flags.add(ResultFlag.ID);//如果是idArg标签则再添加一个flag
      }
      resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
    }
  }

  private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType, List<ResultMapping> resultMappings) {
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String typeHandler = context.getStringAttribute("typeHandler");
    Class<?> javaTypeClass = resolveClass(javaType);
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
    Map<String, String> discriminatorMap = new HashMap<>();
    for (XNode caseChild : context.getChildren()) {
      String value = caseChild.getStringAttribute("value");
      String resultMap = caseChild.getStringAttribute("resultMap", processNestedResultMappings(caseChild, resultMappings, resultType));
      discriminatorMap.put(value, resultMap);
    }
    return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass, discriminatorMap);
  }

  private void sqlElement(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      sqlElement(list, configuration.getDatabaseId());//如果有databaseId设置则传入需要设置的databaseId
    }
    sqlElement(list, null);
  }

  private void sqlElement(List<XNode> list, String requiredDatabaseId) {//如果没有指定databaseId则requiredDatabaseId为null
    for (XNode context : list) {
      String databaseId = context.getStringAttribute("databaseId");//获取SQL标签上的databaseId属性
      String id = context.getStringAttribute("id");//获取sql标签的ID
      id = builderAssistant.applyCurrentNamespace(id, false);//判断该SQL标签ID是否属于当前NAMESPACE，如果没有指定namespace则在id前添加当前的Namespace
      if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {//判断是否符合当前databaseId
        sqlFragments.put(id, context);
      }
    }
  }

  private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
    if (requiredDatabaseId != null) {
      return requiredDatabaseId.equals(databaseId);//判断SQL标签上指定的databaseId是否等于Configuration中指定的databaseId
    }
    if (databaseId != null) {//如果SQL标签上指定了databaseId而Configuration中没有指定则返回false
      return false;
    }
    if (!this.sqlFragments.containsKey(id)) {//Map<String, XNode> sqlFragments不含有该SQL标签
      return true;
    }
    // 如果前一个片段的 databaseId 不为空，则跳过此片段
    XNode context = this.sqlFragments.get(id);//从sqlFragments中获取Xnode
    return context.getStringAttribute("databaseId") == null;
  }

  private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) {
    String property;
    if (flags.contains(ResultFlag.CONSTRUCTOR)) {
      property = context.getStringAttribute("name");//如果该标签中包含CONSTRUCTOR标签则使用name属性作为成员变量名称
    } else {
      property = context.getStringAttribute("property");//不包含CONSTRUCTOR标签则使用property属性作为成员变量名称
    }
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String nestedSelect = context.getStringAttribute("select");//嵌套Select
    String nestedResultMap = context.getStringAttribute("resultMap", () ->
      processNestedResultMappings(context, Collections.emptyList(), resultType));//嵌套ResultMap（association，collection，case标签）使用递归处理（在这里可以得到之前inheritEnclosingType方法的解释）
    String notNullColumn = context.getStringAttribute("notNullColumn");
    String columnPrefix = context.getStringAttribute("columnPrefix");
    String typeHandler = context.getStringAttribute("typeHandler");
    String resultSet = context.getStringAttribute("resultSet");
    String foreignColumn = context.getStringAttribute("foreignColumn");
    boolean lazy = "lazy".equals(context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));//对于ResultMap中可以通过fetchType的设置指定某个关联查询（select属性）进行懒加载，且该属性可以覆盖Configuration中的懒加载属性
    Class<?> javaTypeClass = resolveClass(javaType);//同上
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);//获取TypeHandler
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);//将String转枚举
    return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum, nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet, foreignColumn, lazy);
  }

  private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings, Class<?> enclosingType) {
    if ("association".equals(context.getName())
        || "collection".equals(context.getName())
        || "case".equals(context.getName())) {
      if (context.getStringAttribute("select") == null) {
        validateCollection(context, enclosingType);
        ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
        return resultMap.getId();
      }
    }
    return null;
  }

  protected void validateCollection(XNode context, Class<?> enclosingType) {
    if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
        && context.getStringAttribute("javaType") == null) {
      MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
      String property = context.getStringAttribute("property");
      if (!metaResultType.hasSetter(property)) {
        throw new BuilderException(
          "Ambiguous collection type for property '" + property + "'. You must specify 'javaType' or 'resultMap'.");
      }
    }
  }

  private void bindMapperForNamespace() {//准确的说是将namespace与mapper接口进行绑定
    String namespace = builderAssistant.getCurrentNamespace();
    if (namespace != null) {
      Class<?> boundType = null;
      try {
        boundType = Resources.classForName(namespace);//将Namespace视作为mapper类的全路径名称并加载该类，如果没有加载成功也没关系
      } catch (ClassNotFoundException e) {
        //ignore, bound type is not required
      }
      if (boundType != null) {
        if (!configuration.hasMapper(boundType)) {//向configuration中添加该mapper
          //Spring 可能不知道真正的资源名称，因此我们设置一个标志以防止从映射器界面再次加载此资源查看 MapperAnnotationBuilderloadXmlResource
          // to prevent loading again this resource from the mapper interface
          // look at MapperAnnotationBuilder#loadXmlResource
          configuration.addLoadedResource("namespace:" + namespace);//再次放入loadedResources（已经被加载的资源集合）中（这时候是namespace:mapper的class，之前的是mapper.xml文件）
          configuration.addMapper(boundType);//放入mapperRegistry中
        }
      }
    }
  }

}
