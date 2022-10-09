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
package org.apache.ibatis.builder.xml;

import java.util.List;
import java.util.Locale;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;

/**
 * @author Clinton Begin
 */
public class XMLStatementBuilder extends BaseBuilder {

  private final MapperBuilderAssistant builderAssistant;
  private final XNode context;
  private final String requiredDatabaseId;

  public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context) {
    this(configuration, builderAssistant, context, null);
  }

  public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context, String databaseId) {
    super(configuration);
    this.builderAssistant = builderAssistant;
    this.context = context;
    this.requiredDatabaseId = databaseId;
  }

  public void parseStatementNode() {//解析SQL标签
    String id = context.getStringAttribute("id");//获取SQL语句标签ID
    String databaseId = context.getStringAttribute("databaseId");//获取SQL语句标签上的databaseId要求

    if (!databaseIdMatchesCurrent(id, databaseId, this.requiredDatabaseId)) {//如果SQL语句标签上的databaseId和Configuraiton中的不匹配则直接返回
      return;
    }

    String nodeName = context.getNode().getNodeName();//获取标签名称
    SqlCommandType sqlCommandType = SqlCommandType.valueOf(nodeName.toUpperCase(Locale.ENGLISH));//从标签名称获取SQL类型
    boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
    boolean flushCache = context.getBooleanAttribute("flushCache", !isSelect);//获取flushCache属性，默认值取决于该语句是否是查询语句（查询语句不刷新缓存）
    boolean useCache = context.getBooleanAttribute("useCache", isSelect);//获取useCache属性，默认值取决于是否是查询语句
    boolean resultOrdered = context.getBooleanAttribute("resultOrdered", false);//

    // Include Fragments before parsing
    XMLIncludeTransformer includeParser = new XMLIncludeTransformer(configuration, builderAssistant);//创建include标签XML翻译器
    includeParser.applyIncludes(context.getNode());//解析include标签

    String parameterType = context.getStringAttribute("parameterType");//获取parameterType属性
    Class<?> parameterTypeClass = resolveClass(parameterType);//获取参数类型Class（从别名注册中找）

    String lang = context.getStringAttribute("lang");
    LanguageDriver langDriver = getLanguageDriver(lang);

    // 在包含后解析 selectKey 并删除它们。
    processSelectKeyNodes(id, parameterTypeClass, langDriver);//解析selectKey标签并放入Configuration中

    // 解析 SQL（pre: <selectKey> 和 <include> 被解析并移除）
    KeyGenerator keyGenerator;
    String keyStatementId = id + SelectKeyGenerator.SELECT_KEY_SUFFIX;//public static final String SELECT_KEY_SUFFIX = "!selectKey";
    keyStatementId = builderAssistant.applyCurrentNamespace(keyStatementId, true);
    if (configuration.hasKeyGenerator(keyStatementId)) {//查看Configuration中是否含有对应的keyGenerator
      keyGenerator = configuration.getKeyGenerator(keyStatementId);//获取configuration中的keyGenerator
    } else {
      keyGenerator = context.getBooleanAttribute("useGeneratedKeys",
          configuration.isUseGeneratedKeys() && SqlCommandType.INSERT.equals(sqlCommandType))
          ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;//如果找不到对应的keyGenerator则获取useGeneratedKeys属性默认值是取决于configuration中是否指定使用keyGenerator且目前解析的SQL语句是插入语句，如果useGeneratedKeys是true则返回默认的Jdbc3KeyGenerator.INSTANCE作为keyGenerator否则使用NoKeyGenerator.INSTANCE作为该语句的keyGenerator
    }

    SqlSource sqlSource = langDriver.createSqlSource(configuration, context, parameterTypeClass);
    StatementType statementType = StatementType.valueOf(context.getStringAttribute("statementType", StatementType.PREPARED.toString()));
    Integer fetchSize = context.getIntAttribute("fetchSize");
    Integer timeout = context.getIntAttribute("timeout");
    String parameterMap = context.getStringAttribute("parameterMap");
    String resultType = context.getStringAttribute("resultType");
    Class<?> resultTypeClass = resolveClass(resultType);
    String resultMap = context.getStringAttribute("resultMap");
    String resultSetType = context.getStringAttribute("resultSetType");
    ResultSetType resultSetTypeEnum = resolveResultSetType(resultSetType);
    if (resultSetTypeEnum == null) {
      resultSetTypeEnum = configuration.getDefaultResultSetType();
    }
    String keyProperty = context.getStringAttribute("keyProperty");
    String keyColumn = context.getStringAttribute("keyColumn");
    String resultSets = context.getStringAttribute("resultSets");

    builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
        fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
        resultSetTypeEnum, flushCache, useCache, resultOrdered,
        keyGenerator, keyProperty, keyColumn, databaseId, langDriver, resultSets);
  }

  private void processSelectKeyNodes(String id, Class<?> parameterTypeClass, LanguageDriver langDriver) {
    List<XNode> selectKeyNodes = context.evalNodes("selectKey");//获取selectKey标签
    if (configuration.getDatabaseId() != null) {
      parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, configuration.getDatabaseId());
    }
    parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, null);//解析SelectKey标签
    removeSelectKeyNodes(selectKeyNodes);
  }

  private void parseSelectKeyNodes(String parentId, List<XNode> list, Class<?> parameterTypeClass, LanguageDriver langDriver, String skRequiredDatabaseId) {
    for (XNode nodeToHandle : list) {
      String id = parentId + SelectKeyGenerator.SELECT_KEY_SUFFIX;//parentId就是selectKey标签所在的SQL标签的ID
      String databaseId = nodeToHandle.getStringAttribute("databaseId");
      if (databaseIdMatchesCurrent(id, databaseId, skRequiredDatabaseId)) {//检查DatabaseId是否匹配
        parseSelectKeyNode(id, nodeToHandle, parameterTypeClass, langDriver, databaseId);//解析selectKey标签作为SQLstatement并放入Configuration中
      }
    }
  }

  private void parseSelectKeyNode(String id, XNode nodeToHandle, Class<?> parameterTypeClass, LanguageDriver langDriver, String databaseId) {
    String resultType = nodeToHandle.getStringAttribute("resultType");//获取resultType属性
    Class<?> resultTypeClass = resolveClass(resultType);//找到resultType的class
    StatementType statementType = StatementType.valueOf(nodeToHandle.getStringAttribute("statementType", StatementType.PREPARED.toString()));//获取SQL语句执行类型
    String keyProperty = nodeToHandle.getStringAttribute("keyProperty");//selectKey 语句结果应该被设置到的目标属性。如果生成列不止一个，可以用逗号分隔多个属性名称。
    String keyColumn = nodeToHandle.getStringAttribute("keyColumn");//返回结果集中生成列属性的列名。如果生成列不止一个，可以用逗号分隔多个属性名称。
    boolean executeBefore = "BEFORE".equals(nodeToHandle.getStringAttribute("order", "AFTER"));//order属性

    //defaults
    boolean useCache = false;
    boolean resultOrdered = false;
    KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
    Integer fetchSize = null;
    Integer timeout = null;
    boolean flushCache = false;
    String parameterMap = null;
    String resultMap = null;
    ResultSetType resultSetTypeEnum = null;

    SqlSource sqlSource = langDriver.createSqlSource(configuration, nodeToHandle, parameterTypeClass);//创建selectKey对应的SQLsource
    SqlCommandType sqlCommandType = SqlCommandType.SELECT;

    builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
        fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
        resultSetTypeEnum, flushCache, useCache, resultOrdered,
        keyGenerator, keyProperty, keyColumn, databaseId, langDriver, null);//创建statement并放入configuration中

    id = builderAssistant.applyCurrentNamespace(id, false);

    MappedStatement keyStatement = configuration.getMappedStatement(id, false);//再从Configuration中获取该statement
    configuration.addKeyGenerator(id, new SelectKeyGenerator(keyStatement, executeBefore));//放入keyGenerators集合中（用于保存KeyGenerator）
  }

  private void removeSelectKeyNodes(List<XNode> selectKeyNodes) {
    for (XNode nodeToHandle : selectKeyNodes) {
      nodeToHandle.getParent().getNode().removeChild(nodeToHandle.getNode());
    }
  }

  private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
    if (requiredDatabaseId != null) {//要求Configuration中databaseId与语句要求的databaseId相同
      return requiredDatabaseId.equals(databaseId);
    }
    if (databaseId != null) {
      return false;
    }
    id = builderAssistant.applyCurrentNamespace(id, false);//namespace的拼接
    if (!this.configuration.hasStatement(id, false)) {//如果configuration中不含有statement则直接返回true
      return true;
    }
    // 如果之前有一个不为空的 databaseId，则跳过此语句
    MappedStatement previous = this.configuration.getMappedStatement(id, false); //如果含有该语句且requiredDatabaseId为null且databaseId为null则查看该语句对databaseId的要求
    return previous.getDatabaseId() == null;//如果databaseId为空则返回true
  }

  private LanguageDriver getLanguageDriver(String lang) {
    Class<? extends LanguageDriver> langClass = null;
    if (lang != null) {
      langClass = resolveClass(lang);
    }
    return configuration.getLanguageDriver(langClass);
  }

}
