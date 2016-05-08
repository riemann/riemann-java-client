package riemann.java.client.tests;

import io.riemann.riemann.client.Promise;
import java.lang.Runnable;
import java.lang.Thread;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import java.io.IOException;

public class PromiseTest {

  private Promise<String> p;

  @Before
  public void setUp() {
    p = new Promise<String>();
  }

  @Test
  public void singleTest() throws IOException {
    p.deliver("foo");
    assertEquals("foo", p.deref());
  }

  @Test
  public void threadTest() throws IOException {
    new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          System.out.println("interrupted");
        }
        p.deliver("bar");
      }
    }).start();
    assertEquals("bar", p.deref());
  }

  @Test
  public void timeoutTest() throws IOException {
    assertEquals(null, p.deref(1, TimeUnit.MILLISECONDS));
    assertEquals("failed", p.deref(1, TimeUnit.MILLISECONDS, "failed"));
    
    new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          System.out.println("interrupted");
        }
        p.deliver("done");
      }
    }).start();
    assertEquals("not yet", p.deref(50, TimeUnit.MILLISECONDS, "not yet"));
    assertEquals("done", p.deref(100, TimeUnit.SECONDS));
  }

  @Test
  public void runtimeExceptionTest() throws IOException {
    RuntimeException thrown = null;
    p.deliver(new RuntimeException("fail"));
    try {
      p.deref();
    } catch (RuntimeException e) {
      thrown = e;
    }
    assertEquals("fail", thrown.getMessage());
  }
}
