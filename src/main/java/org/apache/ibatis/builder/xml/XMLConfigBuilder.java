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

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  private boolean parsed;
  private final XPathParser parser;
  private String environment;
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  public Configuration parse() {
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    parsed = true;
    parseConfiguration(parser.evalNode("/configuration"));
    return configuration;
  }

  private void parseConfiguration(XNode root) {
    try {
      //issue #117 read properties first
      propertiesElement(root.evalNode("properties"));
      Properties settings = settingsAsProperties(root.evalNode("settings"));//获取settings标签下的所有子标签
      loadCustomVfs(settings);//VFS属性设置
      loadCustomLogImpl(settings);//设置日志类
      typeAliasesElement(root.evalNode("typeAliases"));//别名注册
      pluginElement(root.evalNode("plugins"));//注册插件
      objectFactoryElement(root.evalNode("objectFactory"));//注册objectFactory
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));//没什么特殊的和上面一样
      reflectorFactoryElement(root.evalNode("reflectorFactory"));//没什么特殊的和上面一样
      settingsElement(settings);//通过settings标签中的内容设置对应值
      // read it after objectFactory and objectWrapperFactory issue #631
      environmentsElement(root.evalNode("environments"));//设置环境
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));//设置数据库ID
      typeHandlerElement(root.evalNode("typeHandlers"));//设置类型处理器
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);//使用MetaClass包装Configuration类
    for (Object key : props.keySet()) {//检查Configuration所有settings标签属性都有set方法
      if (!metaConfig.hasSetter(String.valueOf(key))) {//如果Configuration类中没有该属性的set方法则抛出异常
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;//返回settings标签下的所有子标签
  }

  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    String value = props.getProperty("vfsImpl");//获取vfsImpl属性
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>)Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));//获取logImpl属性指定的类
    configuration.setLogImpl(logImpl);//设置该类作为日志类
  }

  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {//如果采用包名注册
          String typeAliasPackage = child.getStringAttribute("name");//获取包名
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);//注册包下所有类
        } else {
          String alias = child.getStringAttribute("alias");//获取别名
          String type = child.getStringAttribute("type");//获取class路径
          try {
            Class<?> clazz = Resources.classForName(type);//获取该类Class
            if (alias == null) {
              typeAliasRegistry.registerAlias(clazz);//如果别名为空则以type.getSimpleName（）作为别名
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);//如果不为空则注册指定的别名
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        String interceptor = child.getStringAttribute("interceptor");
        Properties properties = child.getChildrenAsProperties();
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor().newInstance();
        interceptorInstance.setProperties(properties);
        configuration.addInterceptor(interceptorInstance);//向interceptorChain中添加该插件
      }
    }
  }
  /*<objectFactory type="org.mybatis.example.ExampleObjectFactory"> <property name="someProperty" value="100"/></objectFactory>*/
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");//获取ObjectFactory的class
      Properties properties = context.getChildrenAsProperties();//获取子标签
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();//通过无参构造方法创建该类（会先从别名注册中查找）
      factory.setProperties(properties);//设置属性值
      configuration.setObjectFactory(factory);//设置Configuration的ObjectFactory
    }
  }

  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setObjectWrapperFactory(factory);
    }
  }

  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setReflectorFactory(factory);
    }
  }

  private void propertiesElement(XNode context) throws Exception {//properties标签中的内容可以通过外界资源文件指定，相当于mybatis.xml文件和外界资源文件的桥梁
    if (context != null) {
      Properties defaults = context.getChildrenAsProperties();//获取Properties标签内所有的属性配置信息
      String resource = context.getStringAttribute("resource");//获取resource属性，该属性存储了外部资源文件的路径，如：<properties resource="org/mybatis/example/config.properties">
      String url = context.getStringAttribute("url");//获取URL属性（url属性是指远程互联网上的属性配置文件）
      if (resource != null && url != null) {//两者只能有其一
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      if (resource != null) {
        defaults.putAll(Resources.getResourceAsProperties(resource));//读取resource指定的属性配置文件
      } else if (url != null) {
        defaults.putAll(Resources.getUrlAsProperties(url));//读取URL指定的属性配置文件
      }
      Properties vars = configuration.getVariables();
      if (vars != null) {
        defaults.putAll(vars);//获取之前的Configuration中variables中的所有配置信息并放入defaults中
      }
      parser.setVariables(defaults);//设置XPathParser中的属性信息为defaults
      configuration.setVariables(defaults);//configuration中的variables就是所有属性配置的保存类
    }
  }

  private void settingsElement(Properties props) {
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
  }

  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {//如果参数中没有指定环境则采用默认环境
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {//遍历所有环境标签
        String id = child.getStringAttribute("id");//获取环境ID
        if (isSpecifiedEnvironment(id)) {//是否是当前环境
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));//解析transactionManager标签（没什么特别的）
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));//解析dataSource标签（没什么特别的）
          DataSource dataSource = dsFactory.getDataSource();//获取dataSource（dataSource是DataSourceFactory的成员变量）
          Environment.Builder environmentBuilder = new Environment.Builder(id)
              .transactionFactory(txFactory)
              .dataSource(dataSource);//简单赋值
          configuration.setEnvironment(environmentBuilder.build());//设置configuration中当前的环境
        }
      }
    }
  }

  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor().newInstance();
      databaseIdProvider.setProperties(properties);
    }
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());//通过datasource获取databaseId
      configuration.setDatabaseId(databaseId);//设置databaseId
    }
  }

  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          String typeHandlerPackage = child.getStringAttribute("name");
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          if (javaTypeClass != null) {
            if (jdbcType == null) {//没有jdbctype的情况（会从注解上找一下，找不到就算了就设置为NULL）
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {//
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {//会从MappedTypes（注解）上找一下，找不到算了
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {//通过package注册（采用Class方式，也就是注解方式）
          String mapperPackage = child.getStringAttribute("name");
          configuration.addMappers(mapperPackage);
        } else {
          String resource = child.getStringAttribute("resource");//从本地文件中获取IO流
          String url = child.getStringAttribute("url");//从远程URL获取
          String mapperClass = child.getStringAttribute("class");//通过CLass获取（注解方式）
          if (resource != null && url == null && mapperClass == null) {//resource方式
            ErrorContext.instance().resource(resource);
            InputStream inputStream = Resources.getResourceAsStream(resource);
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
            mapperParser.parse();//解析Mapper.xml文件，每次解析一个Mapper文件都会检查是否有未解析的ResultMap等
          } else if (resource == null && url != null && mapperClass == null) {//url方式
            ErrorContext.instance().resource(url);
            InputStream inputStream = Resources.getUrlAsStream(url);
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
            mapperParser.parse();
          } else if (resource == null && url == null && mapperClass != null) {//Class方式
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    } else if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    } else if (environment.equals(id)) {
      return true;
    }
    return false;
  }

}
