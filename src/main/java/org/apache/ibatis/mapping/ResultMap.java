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
package org.apache.ibatis.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.reflection.ParamNameUtil;
import org.apache.ibatis.session.Configuration;

/**
 * @author Clinton Begin
 */
public class ResultMap {
  private Configuration configuration;

  private String id;
  private Class<?> type;
  private List<ResultMapping> resultMappings;
  private List<ResultMapping> idResultMappings;
  private List<ResultMapping> constructorResultMappings;
  private List<ResultMapping> propertyResultMappings;
  private Set<String> mappedColumns;
  private Set<String> mappedProperties;
  private Discriminator discriminator;
  private boolean hasNestedResultMaps;
  private boolean hasNestedQueries;
  private Boolean autoMapping;

  private ResultMap() {
  }

  public static class Builder {
    private static final Log log = LogFactory.getLog(Builder.class);

    private ResultMap resultMap = new ResultMap();

    public Builder(Configuration configuration, String id, Class<?> type, List<ResultMapping> resultMappings) {
      this(configuration, id, type, resultMappings, null);
    }

    public Builder(Configuration configuration, String id, Class<?> type, List<ResultMapping> resultMappings, Boolean autoMapping) {
      resultMap.configuration = configuration;
      resultMap.id = id;
      resultMap.type = type;
      resultMap.resultMappings = resultMappings;
      resultMap.autoMapping = autoMapping;
    }

    public Builder discriminator(Discriminator discriminator) {
      resultMap.discriminator = discriminator;
      return this;
    }

    public Class<?> type() {
      return resultMap.type;
    }

    public ResultMap build() {//此时的ResultMap中已经包含了父类中的所有mapping
      if (resultMap.id == null) {
        throw new IllegalArgumentException("ResultMaps must have an id");
      }
      resultMap.mappedColumns = new HashSet<>();
      resultMap.mappedProperties = new HashSet<>();
      resultMap.idResultMappings = new ArrayList<>();
      resultMap.constructorResultMappings = new ArrayList<>();
      resultMap.propertyResultMappings = new ArrayList<>();
      final List<String> constructorArgNames = new ArrayList<>();
      for (ResultMapping resultMapping : resultMap.resultMappings) {//遍历素有Mapping查看是否含有嵌套查询并根据该Mapping的类型放入不同的集合中
        resultMap.hasNestedQueries = resultMap.hasNestedQueries || resultMapping.getNestedQueryId() != null;//只要之前发现有嵌套查询和当前Mapping含有嵌套查询ID
        resultMap.hasNestedResultMaps = resultMap.hasNestedResultMaps || (resultMapping.getNestedResultMapId() != null && resultMapping.getResultSet() == null);//只要含有嵌套ResultMap且不含有ResultSet
        final String column = resultMapping.getColumn();
        if (column != null) {
          resultMap.mappedColumns.add(column.toUpperCase(Locale.ENGLISH));//向存储列名称的集合中添加该列（全大写）
        } else if (resultMapping.isCompositeResult()) {//如果指定的column未空则查看composites集合（用于存储复合列）是否为空
          for (ResultMapping compositeResultMapping : resultMapping.getComposites()) {
            final String compositeColumn = compositeResultMapping.getColumn();
            if (compositeColumn != null) {
              resultMap.mappedColumns.add(compositeColumn.toUpperCase(Locale.ENGLISH));//将所有复合行放入mappedColumns中
            }
          }
        }
        final String property = resultMapping.getProperty();//获取每一条的property属性（成员变量名）
        if (property != null) {
          resultMap.mappedProperties.add(property);//向mappedProperties中添加成员变量名
        }
        if (resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR)) {//如果flags中包含CONSTRUCTOR说明该mapping来自构造标签
          resultMap.constructorResultMappings.add(resultMapping);//constructorResultMappings用于存储构造方法标签内部的标签
          if (resultMapping.getProperty() != null) {
            constructorArgNames.add(resultMapping.getProperty());//constructorArgNames：构造标签中的构造方法参数名称集合
          }
        } else {
          resultMap.propertyResultMappings.add(resultMapping);//如果不是构造方法标签则添加至普通的集合中propertyResultMappings
        }
        if (resultMapping.getFlags().contains(ResultFlag.ID)) {//如果该标签是主键标签则添加至主键mapping集合中
          resultMap.idResultMappings.add(resultMapping);
        }
      }
      if (resultMap.idResultMappings.isEmpty()) {//如果主键集合为空则将所有Mapping放入主键集合中
        resultMap.idResultMappings.addAll(resultMap.resultMappings);
      }
      if (!constructorArgNames.isEmpty()) {//如果构造方法参数不为空则
        final List<String> actualArgNames = argNamesOfMatchingConstructor(constructorArgNames);//检查在ResultMap返回类中能够找到相同的构造方法并返回其参数名称集合
        if (actualArgNames == null) {
          throw new BuilderException("Error in result map '" + resultMap.id
              + "'. Failed to find a constructor in '"
              + resultMap.getType().getName() + "' by arg names " + constructorArgNames
              + ". There might be more info in debug log.");
        }
        resultMap.constructorResultMappings.sort((o1, o2) -> {//根据参数的位置对构造方法参数名称集合进行排序
          int paramIdx1 = actualArgNames.indexOf(o1.getProperty());
          int paramIdx2 = actualArgNames.indexOf(o2.getProperty());
          return paramIdx1 - paramIdx2;
        });
      }
      // 锁定集合
      resultMap.resultMappings = Collections.unmodifiableList(resultMap.resultMappings);
      resultMap.idResultMappings = Collections.unmodifiableList(resultMap.idResultMappings);
      resultMap.constructorResultMappings = Collections.unmodifiableList(resultMap.constructorResultMappings);
      resultMap.propertyResultMappings = Collections.unmodifiableList(resultMap.propertyResultMappings);
      resultMap.mappedColumns = Collections.unmodifiableSet(resultMap.mappedColumns);
      return resultMap;
    }

    private List<String> argNamesOfMatchingConstructor(List<String> constructorArgNames) {
      Constructor<?>[] constructors = resultMap.type.getDeclaredConstructors();//获取resultMap返回类型的所有构造方法
      for (Constructor<?> constructor : constructors) {//遍历构造方法
        Class<?>[] paramTypes = constructor.getParameterTypes();//获取构造方法参数列表类型
        if (constructorArgNames.size() == paramTypes.length) {//找到和构造方法标签参数相同的构造方法
          List<String> paramNames = getArgNames(constructor);//获取该构造方法的参数集合
          if (constructorArgNames.containsAll(paramNames)//如果构造方法标签中指定的参数和实际上构造方法的参数名称一样且类型一样
              && argTypesMatch(constructorArgNames, paramTypes, paramNames)) {
            return paramNames;//返回该构造方法的参数列表
          }
        }
      }
      return null;
    }

    private boolean argTypesMatch(final List<String> constructorArgNames,
        Class<?>[] paramTypes, List<String> paramNames) {
      for (int i = 0; i < constructorArgNames.size(); i++) {
        Class<?> actualType = paramTypes[paramNames.indexOf(constructorArgNames.get(i))];
        Class<?> specifiedType = resultMap.constructorResultMappings.get(i).getJavaType();
        if (!actualType.equals(specifiedType)) {
          if (log.isDebugEnabled()) {
            log.debug("While building result map '" + resultMap.id
                + "', found a constructor with arg names " + constructorArgNames
                + ", but the type of '" + constructorArgNames.get(i)
                + "' did not match. Specified: [" + specifiedType.getName() + "] Declared: ["
                + actualType.getName() + "]");
          }
          return false;
        }
      }
      return true;
    }

    private List<String> getArgNames(Constructor<?> constructor) {
      List<String> paramNames = new ArrayList<>();
      List<String> actualParamNames = null;
      final Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
      int paramCount = paramAnnotations.length;
      for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
        String name = null;
        for (Annotation annotation : paramAnnotations[paramIndex]) {
          if (annotation instanceof Param) {
            name = ((Param) annotation).value();
            break;
          }
        }
        if (name == null && resultMap.configuration.isUseActualParamName()) {
          if (actualParamNames == null) {
            actualParamNames = ParamNameUtil.getParamNames(constructor);
          }
          if (actualParamNames.size() > paramIndex) {
            name = actualParamNames.get(paramIndex);
          }
        }
        paramNames.add(name != null ? name : "arg" + paramIndex);
      }
      return paramNames;
    }
  }

  public String getId() {
    return id;
  }

  public boolean hasNestedResultMaps() {
    return hasNestedResultMaps;
  }

  public boolean hasNestedQueries() {
    return hasNestedQueries;
  }

  public Class<?> getType() {
    return type;
  }

  public List<ResultMapping> getResultMappings() {
    return resultMappings;
  }

  public List<ResultMapping> getConstructorResultMappings() {
    return constructorResultMappings;
  }

  public List<ResultMapping> getPropertyResultMappings() {
    return propertyResultMappings;
  }

  public List<ResultMapping> getIdResultMappings() {
    return idResultMappings;
  }

  public Set<String> getMappedColumns() {
    return mappedColumns;
  }

  public Set<String> getMappedProperties() {
    return mappedProperties;
  }

  public Discriminator getDiscriminator() {
    return discriminator;
  }

  public void forceNestedResultMaps() {
    hasNestedResultMaps = true;
  }

  public Boolean getAutoMapping() {
    return autoMapping;
  }

}
