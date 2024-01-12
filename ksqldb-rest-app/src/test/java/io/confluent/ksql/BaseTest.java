package io.confluent.ksql;

import org.junit.Before;
import org.junit.After;
import org.easymock.internal.LastControl;
public class BaseTest {

  @Before
  public void before(){
    LastControl.pullMatchers();
  }

  @After
  public void after(){
    LastControl.pullMatchers();
  }

}
