package riemann.java.client.tests;

import io.riemann.riemann.client.ChainPromise;
import io.riemann.riemann.client.Promise;
import java.lang.Runnable;
import java.lang.Thread;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import java.io.IOException;

public class ChainPromiseTest {

  private ChainPromise<String> cp;

  @Before
  public void setUp() {
    cp = new ChainPromise<String>();
  }

  @Test
  public void singleTest() throws IOException {
    // A previous version of ChainPromise didn't properly attach the
    // inner promise, this problem was visible in
    // BatchClientPromiseTest
    Promise<String> p = new Promise<String>();
    cp.attach(p);
    p.deliver("foo");
    assertEquals("foo", cp.deref(10, (Object) null));
  }

  @Test
  public void threadTest() throws IOException {
    final Promise<String> p = new Promise<String>();
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
    new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          System.out.println("interrupted");
        }
        cp.attach(p);
      }
    }).start();
    assertEquals("bar", p.deref(200, (Object) null));
  }
}
