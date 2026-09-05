package dev.ricardoleal20.chopflow.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a ChopFlow task handler, discoverable via
 * {@link ChopFlowWorker#scan(Object)}. The annotated method must accept a single
 * {@link com.google.gson.JsonElement} (the payload) and may return a
 * {@link com.google.gson.JsonElement} or any Gson-serializable object.
 *
 * <p>This is the Java analogue of the Celery-style {@code @task} decorator shown in
 * the docs: it lets you co-locate a handler's name with its implementation rather
 * than registering it imperatively.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ChopTask {
  /** The task name this handler responds to (matched against {@code Task.name}). */
  String value();
}
