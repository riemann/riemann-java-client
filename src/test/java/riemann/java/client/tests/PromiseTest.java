package riemann.java.client.tests;

import com.aphyr.riemann.client.Promise;
import java.lang.Runnable;
import java.lang.Thread;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;

public class PromiseTest {

  private Promise<String> p;

  @Before
  public void setUp() {
    p = new Promise<String>();
  }

  @Test
  public void singleTest() {
    p.deliver("foo");
    assertEquals("foo", p.await());
  }

  @Test
  public void threadTest() {
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
    assertEquals("bar", p.await());
  }

  @Test
  public void timeoutTest() {
    assertEquals(null, p.await(1, TimeUnit.MILLISECONDS));
    assertEquals("failed", p.await(1, TimeUnit.MILLISECONDS, "failed"));
    
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
    assertEquals("not yet", p.await(50, TimeUnit.MILLISECONDS, "not yet"));
    assertEquals("done", p.await(100, TimeUnit.SECONDS));
  }

  @Test
  public void exceptionTest() {
    RuntimeException thrown = null;
    p.deliver(new RuntimeException("fail"));
    try {
      p.await();
    } catch (RuntimeException e) {
      thrown = e;
    }
    assertEquals("fail", thrown.getMessage());
  }
}
