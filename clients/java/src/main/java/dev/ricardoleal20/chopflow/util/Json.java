package dev.ricardoleal20.chopflow.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;

/** Small Gson helpers shared by the client and worker. */
public final class Json {
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  /** Parse a JSON string into a {@link JsonElement}. Null/empty → {@link JsonNull}. */
  public static JsonElement parse(String s) {
    if (s == null || s.isEmpty()) return JsonNull.INSTANCE;
    return JsonParser.parseString(s);
  }

  /** Serialize any object (including {@link JsonElement}) to a JSON string. */
  public static String stringify(Object o) {
    return GSON.toJson(o);
  }

  private Json() {}
}
