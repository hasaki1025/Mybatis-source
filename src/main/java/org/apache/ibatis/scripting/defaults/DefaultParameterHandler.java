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
package org.apache.ibatis.scripting.defaults;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeException;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class DefaultParameterHandler implements ParameterHandler {

  private final TypeHandlerRegistry typeHandlerRegistry;

  private final MappedStatement mappedStatement;
  private final Object parameterObject;
  private final BoundSql boundSql;
  private final Configuration configuration;

  public DefaultParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
    this.mappedStatement = mappedStatement;
    this.configuration = mappedStatement.getConfiguration();
    this.typeHandlerRegistry = mappedStatement.getConfiguration().getTypeHandlerRegistry();
    this.parameterObject = parameterObject;
    this.boundSql = boundSql;
  }

  @Override
  public Object getParameterObject() {
    return parameterObject;
  }

  @Override
  public void setParameters(PreparedStatement ps) {
    ErrorContext.instance().activity("setting parameters").object(mappedStatement.getParameterMap().getId());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();//获取参数列表
    if (parameterMappings != null) {
      for (int i = 0; i < parameterMappings.size(); i++) {
        ParameterMapping parameterMapping = parameterMappings.get(i);
        if (parameterMapping.getMode() != ParameterMode.OUT) {//如果参数类型不是输出类型
          Object value;
          String propertyName = parameterMapping.getProperty();//获取参数名称
          if (boundSql.hasAdditionalParameter(propertyName)) { // 是否含有额外的参数
            value = boundSql.getAdditionalParameter(propertyName);
          } else if (parameterObject == null) {
            value = null;
          } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {//如果含有参数列表的Handler则赋值
            value = parameterObject;
          } else {
            MetaObject metaObject = configuration.newMetaObject(parameterObject);//使用MetaObject包装参数类
            value = metaObject.getValue(propertyName);//赋值
          }
          TypeHandler typeHandler = parameterMapping.getTypeHandler();//获取参数映射的处理类
          JdbcType jdbcType = parameterMapping.getJdbcType();//获取JdbcType
          if (value == null && jdbcType == null) {
            jdbcType = configuration.getJdbcTypeForNull();//如果参数为空且jdbcType为空则从configuration中获取JDBC空类型并设置给jdbcType
          }
          try {
            typeHandler.setParameter(ps, i + 1, value, jdbcType);//设置参数
          } catch (TypeException | SQLException e) {
            throw new TypeException("Could not set parameters for mapping: " + parameterMapping + ". Cause: " + e, e);
          }
        }
      }
    }
  }

}
