package io.confluent.ksql.rest.server;

import com.google.common.base.Preconditions;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Wrapper around a TestKsqlRestApp that allows the application setup to be skipped.  Useful for
 * writing integration tests where hosts are setup in one place, but where different tests might
 * want an arbitrary subset of the hosts to be setup.
 */
public class SkippableTestKsqlRestAppWrapper extends ExternalResource {

  private final TestKsqlRestApp app;
  private final String name;
  private boolean skipped;

  public SkippableTestKsqlRestAppWrapper(TestKsqlRestApp app, String name) {
    this.app = app;
    this.name = name;
    this.skipped = false;
  }

  @Override
  public Statement apply(Statement statement, Description description) {
    System.out.println("SkippableTestKsqlRestAppWrapper");
    SkipHost skipHost = description.getAnnotation(SkipHost.class);
    if (!(skipHost != null && Arrays.asList(skipHost.names()).contains(name))) {
      return app.apply(statement, description);
    }
    // Do nothing
    skipped = true;
    return super.apply(statement, description);
  }

  public TestKsqlRestApp get() {
    Preconditions.checkState(!skipped, "Should only access non skipped application");
    return app;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  public @interface SkipHost {
    String[] names();
  }
}