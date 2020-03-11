//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.common.util;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * This is a customized runtime type adapter factory to handle exceptions. So that
 * the exception types of the exceptions are kept during the JSON Ser/De process
 */
public final class RuntimeTypeAdapterFactory<T> implements TypeAdapterFactory {
  private final Class<?> baseType;
  private final String typeFieldName;
  private final Map<String, Class<?>> labelToSubtype = new LinkedHashMap<String, Class<?>>();
  private final Map<Class<?>, String> subtypeToLabel = new LinkedHashMap<Class<?>, String>();
  private final boolean maintainType;

  private RuntimeTypeAdapterFactory(Class<?> baseType, String typeFieldName, boolean maintainType) {
    if (typeFieldName == null || baseType == null) {
      throw new NullPointerException();
    }
    this.baseType = baseType;
    this.typeFieldName = typeFieldName;
    this.maintainType = maintainType;
  }

  /**
   * Creates a new runtime type adapter using for {@code baseType} using {@code
   * typeFieldName} as the type field name. Type field names are case sensitive.
   * {@code maintainType} flag decide if the type will be stored in pojo or not.
   */
  public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType,
                                                    String typeFieldName, boolean maintainType) {
    return new RuntimeTypeAdapterFactory<T>(baseType, typeFieldName, maintainType);
  }

  /**
   * Creates a new runtime type adapter using for {@code baseType} using {@code
   * typeFieldName} as the type field name. Type field names are case sensitive.
   */
  public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType, String typeFieldName) {
    return new RuntimeTypeAdapterFactory<T>(baseType, typeFieldName, false);
  }

  /**
   * Creates a new runtime type adapter for {@code baseType} using {@code "type"} as
   * the type field name.
   */
  public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType) {
    return new RuntimeTypeAdapterFactory<T>(baseType, "type", false);
  }

  /**
   * Registers {@code type} identified by {@code label}. Labels are case
   * sensitive.
   *
   * @throws IllegalArgumentException if either {@code type} or {@code label}
   * have already been registered on this type adapter.
   */
  public RuntimeTypeAdapterFactory<T> registerSubtype(Class<? extends T> type, String label) {
    if (type == null || label == null) {
      throw new NullPointerException();
    }
    if (subtypeToLabel.containsKey(type) || labelToSubtype.containsKey(label)) {
      throw new IllegalArgumentException("types and labels must be unique");
    }
    labelToSubtype.put(label, type);
    subtypeToLabel.put(type, label);
    return this;
  }

  /**
   * Registers {@code type} identified by its {@link Class#getSimpleName simple
   * name}. Labels are case sensitive.
   *
   * @throws IllegalArgumentException if either {@code type} or its simple name
   * have already been registered on this type adapter.
   */
  public RuntimeTypeAdapterFactory<T> registerSubtype(Class<? extends T> type) {
    return registerSubtype(type, type.getSimpleName());
  }

  /**
   * doc.
   */
  public <R> TypeAdapter<R> create(Gson gson, TypeToken<R> type) {
    if (type.getRawType() != baseType) {
      return null;
    }

    final Map<String, TypeAdapter<?>> labelToDelegate
        = new LinkedHashMap<String, TypeAdapter<?>>();
    final Map<Class<?>, TypeAdapter<?>> subtypeToDelegate
        = new LinkedHashMap<Class<?>, TypeAdapter<?>>();
    for (Map.Entry<String, Class<?>> entry : labelToSubtype.entrySet()) {
      TypeAdapter<?> delegate = gson
          .getDelegateAdapter(this, TypeToken.get(entry.getValue()));
      labelToDelegate.put(entry.getKey(), delegate);
      subtypeToDelegate.put(entry.getValue(), delegate);
    }

    return new TypeAdapter<R>() {
      @Override
      public R read(JsonReader in) throws IOException {
        JsonElement jsonElement = Streams.parse(in);
        JsonElement labelJsonElement;
        if (maintainType) {
          labelJsonElement = jsonElement.getAsJsonObject().get(typeFieldName);
        } else {
          labelJsonElement = jsonElement.getAsJsonObject().remove(typeFieldName);
        }

        if (labelJsonElement == null) {
          throw new JsonParseException("cannot deserialize " + baseType
              + " because it does not define a field named " + typeFieldName);
        }
        String label = labelJsonElement.getAsString();
        if (!labelToDelegate.containsKey(label)) {
          label = "exex";
        }
        @SuppressWarnings("unchecked") // registration requires that subtype extends T
            TypeAdapter<R> delegate = (TypeAdapter<R>) labelToDelegate.get(label);
        if (delegate == null) {
          throw new JsonParseException("cannot deserialize " + baseType + " subtype named "
              + label + "; did you forget to register a subtype?");
        }
        R result = delegate.fromJsonTree(jsonElement);
        if (result instanceof Exception) {
          JsonElement causeElement;
          Exception cause;

          causeElement = jsonElement.getAsJsonObject().get("cause");
          if (maintainType) {
            labelJsonElement = causeElement.getAsJsonObject().get(typeFieldName);
          } else {
            labelJsonElement = causeElement.getAsJsonObject().remove(typeFieldName);
          }
          if (labelJsonElement == null) {
            throw new JsonParseException("cannot deserialize " + baseType
                + " because it does not define a field named " + typeFieldName);
          }
          label = labelJsonElement.getAsString();
          if (!labelToDelegate.containsKey(label)) {
            label = "exex";
          }
          TypeAdapter<Exception> causeDelegate
              = (TypeAdapter<Exception>) labelToDelegate.get(label);
          if (causeDelegate == null) {
            throw new JsonParseException("cannot deserialize " + baseType + " subtype named "
                + label + "; did you forget to register a subtype?");
          }
          cause = causeDelegate.fromJsonTree(causeElement);
          Exception exception = new Exception(((Exception) result).getMessage(), cause);
          exception.setStackTrace(((Exception) result).getStackTrace());
          return (R) exception;
        } else {
          return result;
        }
      }

      @Override
      public void write(JsonWriter out, R value) throws IOException {
        Class<?> srcType = value.getClass();
        String label = subtypeToLabel.get(srcType);
        @SuppressWarnings("unchecked") // registration requires that subtype extends T
            TypeAdapter<R> delegate = getrTypeAdapter(value, srcType);
        JsonObject jsonObject = delegate.toJsonTree(value).getAsJsonObject();

        //hack to convert the cause with the correct type
        if (value instanceof Exception && ((Exception) value).getCause() != null) {
          Exception cause = (Exception) ((Exception) value).getCause();
          Class<?> causeSrcType = ((Exception) value).getCause().getClass();
          String causelabel = subtypeToLabel.get(causeSrcType);
          @SuppressWarnings("unchecked") // registration requires that subtype extends T
              TypeAdapter<Exception> causedelegate
              = getrTypeAdapter(cause, causeSrcType);

          JsonObject jsonObjectCause = causedelegate.toJsonTree(cause).getAsJsonObject();
          jsonObjectCause.add(typeFieldName, new JsonPrimitive(causelabel));
          jsonObject.remove("cause");
          jsonObject.add("cause", jsonObjectCause);
        }
        if (maintainType) {
          Streams.write(jsonObject, out);
          return;
        }

        JsonObject clone = new JsonObject();

        if (jsonObject.has(typeFieldName)) {
          throw new JsonParseException("cannot serialize " + srcType.getName()
              + " because it already defines a field named " + typeFieldName);
        }
        clone.add(typeFieldName, new JsonPrimitive(label));

        for (Map.Entry<String, JsonElement> e : jsonObject.entrySet()) {
          clone.add(e.getKey(), e.getValue());
        }
        Streams.write(clone, out);
      }

      private <T> TypeAdapter<T> getrTypeAdapter(T value,

                                                 Class<?> srcType) {
        TypeAdapter<T> delegate = (TypeAdapter<T>) subtypeToDelegate.get(srcType);
        if (delegate == null) {
          if (value instanceof Exception) {
            delegate = (TypeAdapter<T>) subtypeToDelegate.get(Exception.class);
            if (delegate == null) {
              throw new JsonParseException("cannot serialize " + srcType.getName()
                  + "; did you forget to register Exception as a subtype?");
            }
          } else {
            throw new JsonParseException("cannot serialize " + srcType.getName()
                + "; did you forget to register a subtype?");
          }
        }
        return delegate;
      }
    }.nullSafe();
  }
}
