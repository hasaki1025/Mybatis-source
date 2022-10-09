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
package org.apache.ibatis.executor;

import java.lang.reflect.Array;
import java.util.List;

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;

/**
 * @author Andrew Gustafson
 */
public class ResultExtractor {
  private final Configuration configuration;
  private final ObjectFactory objectFactory;

  public ResultExtractor(Configuration configuration, ObjectFactory objectFactory) {
    this.configuration = configuration;
    this.objectFactory = objectFactory;
  }

  public Object extractObjectFromList(List<Object> list, Class<?> targetType) {
    Object value = null;
    if (targetType != null && targetType.isAssignableFrom(list.getClass())) {//查看结果类型是否可以转化换为List集合，如果可以直接赋值value
      value = list;
    } else if (targetType != null && objectFactory.isCollection(targetType)) {//判断返回的结果类型是否是Collection
      value = objectFactory.create(targetType);//通过对象工厂创建该类型
      MetaObject metaObject = configuration.newMetaObject(value);//使用MetaObject封装value
      metaObject.addAll(list);//添加所有结果
    } else if (targetType != null && targetType.isArray()) {//判断是否是数组类型
      Class<?> arrayComponentType = targetType.getComponentType();//getComponentType:可以获取一个数组类型的Class
      Object array = Array.newInstance(arrayComponentType, list.size());//Array.newInstance方法可以通过数组类型的Class创建对应的数组
      if (arrayComponentType.isPrimitive()) {//isPrimitive用于判别一个类是否是基础类（int等或者基础数组）
        for (int i = 0; i < list.size(); i++) {
          Array.set(array, i, list.get(i));//将结果给数组赋值
        }
        value = array;
      } else {
        value = list.toArray((Object[])array);//如果不是基础类型则直接将结果list转换为数组
      }
    } else {
      if (list != null && list.size() > 1) {
        throw new ExecutorException("Statement returned more than one row, where no more than one was expected.");
      } else if (list != null && list.size() == 1) {
        value = list.get(0);//只有一条结果直接返回值
      }
    }
    return value;
  }
}
