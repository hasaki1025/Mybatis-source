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
package org.apache.ibatis.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

public class ParamNameResolver {

  public static final String GENERIC_NAME_PREFIX = "param";

  /**
   * <p>
   * The key is the index and the value is the name of the parameter.<br />
   * The name is obtained from {@link Param} if specified. When {@link Param} is not specified,
   * the parameter index is used. Note that this index could be different from the actual index
   * when the method has special parameters (i.e. {@link RowBounds} or {@link ResultHandler}).
   * </p>
   * <ul>
   * <li>aMethod(@Param("M") int a, @Param("N") int b) -&gt; {{0, "M"}, {1, "N"}}</li>
   * <li>aMethod(int a, int b) -&gt; {{0, "0"}, {1, "1"}}</li>
   * <li>aMethod(int a, RowBounds rb, int b) -&gt; {{0, "0"}, {2, "1"}}</li>
   * </ul>
   */
  private final SortedMap<Integer, String> names;

  private boolean hasParamAnnotation;

  public ParamNameResolver(Configuration config, Method method) {
    final Class<?>[] paramTypes = method.getParameterTypes();//获取参数列表的Class
    final Annotation[][] paramAnnotations = method.getParameterAnnotations();//获取参数上的注解，方便判断是否含有Param注解
    final SortedMap<Integer, String> map = new TreeMap<>();
    int paramCount = paramAnnotations.length;
    // get names from @Param annotations
    for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
      if (isSpecialParameter(paramTypes[paramIndex])) {//判断该参数是否是特殊参数（如结果处理器、分页类）
        // skip special parameters
        continue;
      }
      String name = null;
      for (Annotation annotation : paramAnnotations[paramIndex]) {
        if (annotation instanceof Param) {//判断是否该参数上存在Param注解
          hasParamAnnotation = true;
          name = ((Param) annotation).value();
          break;
        }
      }
      if (name == null) {//没有Param注解的情况
        // @Param was not specified.
        if (config.isUseActualParamName()) {//查看configruation中是否指定采用方法签名上的原生参数名
          name = getActualParamName(method, paramIndex);
        }
        if (name == null) {
          // use the parameter index as the name ("0", "1", ...)
          // gcode issue #71
          name = String.valueOf(map.size());//如果configruation中指定不采用方法签名上的原生参数名则使用该参数在参数列表中的顺序编号作为参数名称
        }
      }
      map.put(paramIndex, name);//放入Map集合中
    }
    names = Collections.unmodifiableSortedMap(map);//初始化Names
  }

  private String getActualParamName(Method method, int paramIndex) {
    return ParamNameUtil.getParamNames(method).get(paramIndex);
  }

  private static boolean isSpecialParameter(Class<?> clazz) {
    return RowBounds.class.isAssignableFrom(clazz) || ResultHandler.class.isAssignableFrom(clazz);
  }

  /**
   * Returns parameter names referenced by SQL providers.
   */
  public String[] getNames() {
    return names.values().toArray(new String[0]);
  }

  /**
   * <p>
   * A single non-special parameter is returned without a name.
   * Multiple parameters are named using the naming rule.
   * In addition to the default names, this method also adds the generic names (param1, param2,
   * ...).
   * </p>
   */
  public Object getNamedParams(Object[] args) {
    final int paramCount = names.size();//获取参数数量
    if (args == null || paramCount == 0) {//没有参数直接返回空
      return null;
    } else if (!hasParamAnnotation && paramCount == 1) {//一个参数直接返回第一个参数
      return args[names.firstKey()];
    } else {//多个参数则使用Map封装
      final Map<String, Object> param = new ParamMap<>();
      int i = 0;
      for (Map.Entry<Integer, String> entry : names.entrySet()) {
        param.put(entry.getValue(), args[entry.getKey()]);//根据之前参数解析时获取的参数名称和参数位置的Map构建参数的映射
        // 添加通用参数名称（param1，param2，...）
        final String genericParamName = GENERIC_NAME_PREFIX + (i + 1);
        // 确保不覆盖以@Param 命名的参数
        if (!names.containsValue(genericParamName)) {
          param.put(genericParamName, args[entry.getKey()]);
        }
        i++;
      }
      return param;
    }
  }
}
