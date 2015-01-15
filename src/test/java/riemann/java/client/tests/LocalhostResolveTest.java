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

    private static Map<String, String> env = new HashMap<String, String>();

    private static final String OS_NAME_PROP = "os.name";
    private static final String EXPECTED_ENV_LOCALHOST = "env_localhost";

    @BeforeClass
    public static void oneTimeSetUp() {
        // clear env vars
        env.remove(LocalhostResolver.HOSTNAME);
        env.remove(LocalhostResolver.COMPUTERNAME);
        setEnv(env);

        LocalhostResolver.refreshIntervalMillis = 1000;
    }

    @Before
    public void setup() {
        LocalhostResolver.setLastUpdateTime(0);
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
    }

    @Test
    public void testUpdateInterval() {
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
        String hostname = LocalhostResolver.getResolvedHostname();
        long lastUpdateTime = LocalhostResolver.getLastUpdateTime();
        Assert.assertNotNull(hostname);

        // simulate time passing
        LocalhostResolver.setLastUpdateTime(lastUpdateTime - (LocalhostResolver.refreshIntervalMillis + 500));

        Assert.assertNotSame(lastUpdateTime, LocalhostResolver.getLastUpdateTime());
    }

    @Test
    public void testCaching() {
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
        String hostname1 = LocalhostResolver.getResolvedHostname();
        long updateTime1 = LocalhostResolver.getLastUpdateTime();
        Assert.assertNotNull(hostname1);

        String hostname2 = LocalhostResolver.getResolvedHostname();
        long updateTime2 = LocalhostResolver.getLastUpdateTime();

        Assert.assertEquals(hostname1, hostname2);
        Assert.assertEquals(updateTime1, updateTime2);
    }

    @Test
    public void testNoEnvVars() throws UnknownHostException {
        String hostname = LocalhostResolver.getResolvedHostname();
        long lastUpdateTime = LocalhostResolver.getLastUpdateTime();
        Assert.assertNotNull(hostname);
        Assert.assertNotSame(0, lastUpdateTime);

        // ensure queried hostname without env vars
        Assert.assertEquals(java.net.InetAddress.getLocalHost().getHostName(), hostname);
    }

    @Test
    public void testEnvVarWindows() {
        System.getProperties().put(OS_NAME_PROP, "Windows7");
        env = new HashMap<String, String>(System.getenv());
        env.put(LocalhostResolver.COMPUTERNAME, EXPECTED_ENV_LOCALHOST);
        setEnv(env);
        LocalhostResolver.resolveByEnv(); // force re-init

        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertEquals(EXPECTED_ENV_LOCALHOST, hostname);
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
    }

    @Test
    public void testEnvVarNix() {
        env = new HashMap<String, String>(System.getenv());
        env.put(LocalhostResolver.HOSTNAME, EXPECTED_ENV_LOCALHOST);
        setEnv(env);
        LocalhostResolver.resolveByEnv(); // force re-init

        System.getProperties().put(OS_NAME_PROP, "Linux");
        String hostname = LocalhostResolver.getResolvedHostname();
        Assert.assertEquals(EXPECTED_ENV_LOCALHOST, hostname);
        Assert.assertEquals(0, LocalhostResolver.getLastUpdateTime());
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
                throw new RuntimeException(e2);
            }
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }
    }
}
