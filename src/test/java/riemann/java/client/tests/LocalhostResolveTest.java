package riemann.java.client.tests;

import com.aphyr.riemann.client.LocalhostResolver;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LocalhostResolveTest {

    protected Map<String, String> env = new HashMap<String, String>();

    protected static final String OS_NAME_PROP = "os.name";
    protected static final String ExpectedWinHostname = "LocalHostResolverTestWin";
    protected static final String ExpectedNixHostname = "LocalHostResolverTestNix";

    @BeforeClass
    public static void oneTimeSetUp() {
        LocalhostResolver.RefreshIntervalMillis = 1000;
    }

    @Before
    public void setup() {
        env = new HashMap<String, String>(System.getenv());
        env.put(LocalhostResolver.HOSTNAME, ExpectedNixHostname);
        env.put(LocalhostResolver.COMPUTERNAME, ExpectedWinHostname);
        setEnv(env);

        LocalhostResolver.CustomEnvVarName = null;
        LocalhostResolver.resetUpdateTimes();
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
        Assert.assertEquals(0, LocalhostResolver.getLastNetUpdateTime());
    }

    @Test
    public void testUpdateInterval() {
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
        String hostname = LocalhostResolver.getResolvedHostname();
        long lastUpdateTime = LocalhostResolver.getLastUpdateTime();
        Assert.assertNotNull(hostname);
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotSame(lastUpdateTime, LocalhostResolver.getLastUpdateTime());
    }

    @Test
    public void testNoEnvVars() throws UnknownHostException {
        env.remove(LocalhostResolver.HOSTNAME);
        env.remove(LocalhostResolver.COMPUTERNAME);
        setEnv(env);

        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertNotNull(hostname);

        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // ensure queried hostname without env vars
        Assert.assertEquals(LocalhostResolver.getLastUpdateTime(), LocalhostResolver.getLastNetUpdateTime());
        Assert.assertEquals(java.net.InetAddress.getLocalHost().getHostName(), hostname);
    }

    @Test
    public void testEnvVarWindows() {
        System.getProperties().put(OS_NAME_PROP, "Windows7");

        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertEquals(ExpectedWinHostname, hostname);
        Assert.assertEquals(0, LocalhostResolver.getLastNetUpdateTime());
    }

    @Test
    public void testEnvVarNix() {
        System.getProperties().put(OS_NAME_PROP, "Linux");
        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertEquals(ExpectedNixHostname, hostname);
        Assert.assertEquals(0, LocalhostResolver.getLastNetUpdateTime());
    }

    @Test
    public void testCustomEnvVar() {
        final String customHostnameEnvVar = "AWS_HOST";
        final String customHostname = "EC2-LocalHostResolverTest";
        env.put(customHostnameEnvVar, customHostname);
        setEnv(env);

        LocalhostResolver.CustomEnvVarName = customHostnameEnvVar;
        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertEquals(customHostname, hostname);
        Assert.assertEquals(0, LocalhostResolver.getLastNetUpdateTime());
    }


    /**
     * evil hack for testing (only!) with env var in-memory modification
     * see: http://stackoverflow.com/a/7201825/1469004
     *
     * @param newEnv - to set in memory
     */
    protected static void setEnv(Map<String, String> newEnv) {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(newEnv);
            Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll(newEnv);
        }
        catch (NoSuchFieldException e) {
            try {
                Class[] classes = Collections.class.getDeclaredClasses();
                Map<String, String> env = System.getenv();
                for(Class cl : classes) {
                    if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                        Field field = cl.getDeclaredField("m");
                        field.setAccessible(true);
                        Object obj = field.get(env);
                        Map<String, String> map = (Map<String, String>) obj;
                        map.clear();
                        map.putAll(newEnv);
                    }
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}
