/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.cassandra.utils.FBUtilities;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.FieldProperty;
import org.yaml.snakeyaml.introspector.MethodProperty;
import org.yaml.snakeyaml.introspector.Property;

import static org.apache.cassandra.utils.FBUtilities.camelToSnake;

public class DefaultLoader implements Loader
{
    @Override
    public Map<String, Property> getProperties(Class<?> root)
    {
        Map<String, Property> properties = new HashMap<>();
        for (Class<?> c = root; c != null; c = c.getSuperclass())
        {
            for (Field f : c.getDeclaredFields())
            {
                String name = camelToSnake(f.getName());
                int modifiers = f.getModifiers();
                if (!Modifier.isStatic(modifiers)
                    && !f.isAnnotationPresent(JsonIgnore.class)
                    && !Modifier.isTransient(modifiers)
                    && Modifier.isPublic(modifiers)
                    && !properties.containsKey(name))
                    properties.put(name, new FieldProperty(f));
            }
        }
        try
        {
            PropertyDescriptor[] descriptors = Introspector.getBeanInfo(root).getPropertyDescriptors();
            if (descriptors != null)
            {
                for (PropertyDescriptor d : descriptors)
                {
                    String name = camelToSnake(d.getName());
                    Method writeMethod = d.getWriteMethod();
                    // if the property can't be written to, then ignore it
                    if (writeMethod == null || writeMethod.isAnnotationPresent(JsonIgnore.class))
                        continue;
                    // if read method exists, override the field version in case get/set does validation
                    if (properties.containsKey(name) && (d.getReadMethod() == null || d.getReadMethod().isAnnotationPresent(JsonIgnore.class)))
                        continue;
                    d.setName(name);
                    properties.put(name, new MethodPropertyPlus(d));
                }
            }
        }
        catch (IntrospectionException e)
        {
            throw new RuntimeException(e);
        }
        Map<String, ListenableProperty<?, ?>> listenable = new HashMap<>();
        for (Map.Entry<String, Property> e : properties.entrySet())
        {
            Property delegate = e.getValue();
            ValidateList vl = delegate.getAnnotation(ValidateList.class);
            if (vl != null && vl.value().length > 0)
            {
                listenable.put(e.getKey(), addValidations(delegate, vl.value()));
            }
            else
            {
                Validate v = delegate.getAnnotation(Validate.class);
                if (v != null)
                    listenable.put(e.getKey(), addValidations(delegate, new Validate[] {v}));
            }
        }
        if (!listenable.isEmpty())
            properties.putAll(listenable); // override
        return properties;
    }

    private static ListenableProperty<?, ?> addValidations(Property delegate, Validate[] values)
    {
        ListenableProperty<?, ?> prop = new ListenableProperty<>(delegate);
        for (Validate v : values)
            add(prop, v);
        return prop;
    }

    private static void add(ListenableProperty<?, ?> prop, Validate validate)
    {
        String klassName = validate.klass();
        if (Strings.isNullOrEmpty(validate.method()))
        {
            prop.add(klassName, FBUtilities.construct(klassName, "ConfigListener"));
        }
        else
        {
            Class<?> klass = FBUtilities.classForName(klassName, "ConfigListener");
            List<Method> matches = new ArrayList<>();
            for (Method method : klass.getDeclaredMethods())
            {
                if (method.getName().equals(validate.method()) && Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()))
                    matches.add(method);
            }
            switch (matches.size())
            {
                case 0:
                    throw new IllegalArgumentException(String.format("Validate for %s asked for class %s and method %s, but could not find method", prop, klass, validate.method()));
                case 1:
                    add(prop, matches.get(0));
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Validate for %s asked for class %s and method %s, but found %d matches; need to limit to 1", prop, klass, validate.method(), matches.size()));
            }
        }
    }

    private static boolean add(ListenableProperty<?, ?> prop, Method method)
    {
        String name = method.getDeclaringClass().getCanonicalName() + "#" + method.getName();
        switch (method.getParameterCount())
        {
            case 3:
                return prop.add(name, new AllArgsListener<>(method));
            default:
                throw new IllegalArgumentException("Listenable with argument count " + method.getParameterCount() + " not supported yet");
        }
    }

    /**
     * .get() acts differently than .set() and doesn't do a good job showing the cause of the failure, this
     * class rewrites to make the errors easier to reason with.
     */
    private static class MethodPropertyPlus extends MethodProperty
    {
        private final Method readMethod;

        public MethodPropertyPlus(PropertyDescriptor property)
        {
            super(property);
            this.readMethod = property.getReadMethod();
        }

        @Override
        public Object get(Object object)
        {
            if (!isReadable())
                throw new YAMLException("No readable property '" + getName() + "' on class: " + object.getClass().getName());

            try
            {
                return readMethod.invoke(object);
            }
            catch (IllegalAccessException e)
            {
                throw new YAMLException("Unable to find getter for property '" + getName() + "' on class " + object.getClass().getName(), e);
            }
            catch (InvocationTargetException e)
            {
                throw new YAMLException("Failed calling getter for property '" + getName() + "' on class " + object.getClass().getName(), e.getCause());
            }
        }
    }

    private static class AllArgsListener<A, B> implements ConfigListener<A, B>
    {
        private final Method method;

        public AllArgsListener(Method method)
        {
            this.method = method;
        }

        @Override
        public B visit(A object, String name, B value)
        {
            try
            {
                Object result = method.invoke(null, object, name, value);
                return method.getReturnType() == Void.TYPE ? value : (B) result;
            }
            catch (IllegalAccessException e)
            {
                throw new AssertionError("Unable to access public static method", e);
            }
            catch (InvocationTargetException e)
            {
                throw new IllegalArgumentException(e.getTargetException());
            }
        }
    }
}
