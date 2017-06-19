package com.jzy.redis.util;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.chainsaw.Main;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;  
import redis.clients.jedis.JedisPoolConfig;  
  
/** 
 * Redis 工具类 
 */  
public class JedisUtil {  
  
    protected static ReentrantLock lockPool = new ReentrantLock();  
    protected static ReentrantLock lockJedis = new ReentrantLock();  
  
    protected static Logger logger = Logger.getLogger(JedisUtil.class);  
  
    //Redis服务器IP  
    private static String ADDR_ARRAY = "xxx.xxx.xxx.xxx";  
  
    //Redis的端口号  
    private static int PORT = 7000;  
  
    
    //可用连接实例的最大数目，默认值为8；  
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。  
    private static int MAX_ACTIVE = 150;  
  
    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。  
    private static int MAX_IDLE = 30;  
    private static int MIN_IDLE = 10;  
  
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；  
    private static int MAX_WAIT = 3000;  
  
    //超时时间  
    private static int TIMEOUT = 30000;  
  
    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；  
    private static boolean TEST_ON_BORROW = false;  
  
    private static JedisPool jedisPool = null;  
  
    /** 
     * redis过期时间,以秒为单位 
     */  
    public final static int EXRP_HOUR = 60 * 60;            //一小时  
    public final static int EXRP_DAY = 60 * 60 * 24;        //一天  
    public final static int EXRP_MONTH = 60 * 60 * 24 * 30; //一个月  
  
    /** 
     * 初始化Redis连接池 
     */  
    private static void initialPool(String addrArray,int port) {  
        try {  
            JedisPoolConfig config = new JedisPoolConfig();  
            config.setMaxTotal(MAX_ACTIVE);  
            config.setMaxIdle(MAX_IDLE);
            config.setMinIdle(MAX_IDLE); 
            config.setMaxWaitMillis(MAX_WAIT);  
            // 在borrow一个jedis实例时，是否提前进行alidate操作；如果为true，则得到的jedis实例均是可用的；  
            config.setTestOnBorrow(TEST_ON_BORROW); 
            jedisPool = new JedisPool(config, addrArray.split(",")[0], port, TIMEOUT);  
        } catch (Exception e) {  
            logger.error("First create JedisPool error : " + e);  
            try {  
                //如果第一个IP异常，则访问第二个IP  
                JedisPoolConfig config = new JedisPoolConfig();  
                config.setMaxTotal(MAX_ACTIVE);  
                config.setMaxIdle(MAX_IDLE);  
                config.setMinIdle(MAX_IDLE); 
                config.setMaxWaitMillis(MAX_WAIT);  
                config.setTestOnBorrow(TEST_ON_BORROW);  
                jedisPool = new JedisPool(config, addrArray.split(",")[1], port, TIMEOUT);  
            } catch (Exception e2) {  
                logger.error("Second create JedisPool error : " + e2);  
            }  
        }  
    }  
  
    /** 
     * 在多线程环境同步初始化 
     */  
    private static void poolInit(String addrArray,int port) {  
        lockPool.lock();  
        try {  
            if (jedisPool == null) {  
                initialPool(addrArray, port);  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            lockPool.unlock();  
        }  
    }  
  
    public static Jedis getJedis(String addrArray,int port) {  
        lockJedis.lock();  
        if (jedisPool == null) {  
            poolInit(addrArray,port);  
        }  
        Jedis jedis = null;  
        try {  
            if (jedisPool != null) {  
                jedis = jedisPool.getResource();  
            }  
        } catch (Exception e) {  
            logger.error("Get jedis error : " + e);  
        } finally {  
            returnResource(jedis);  
            lockJedis.unlock();  
        }  
        return jedis;  
    }  
  
    /** 
     * 释放jedis资源 
     * 
     * @param jedis 
     */  
    public static void returnResource(final Jedis jedis) {  
        if (jedis != null && jedisPool != null) {  
            jedisPool.returnResource(jedis);  
        }  
    }  
    
/*    public static void main(String[] args) {
    	Jedis jedis=JedisUtil.getJedis("192.168.1.136", 7000);
    	jedis.set("c093a57347204e83bb8f6141e9081724_jzy_test", "asdasdasdas");
    	String str=jedis.get("c093a57347204e83bb8f6141e9081724_jzy_test");
    	System.out.println(str);
    	
	}*/
  
    public static void main(String[] args) {  
        JedisPoolConfig poolConfig = new JedisPoolConfig();  
        // 最大连接数  
        poolConfig.setMaxTotal(1);  
        // 最大空闲数  
        poolConfig.setMaxIdle(1);  
        // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：  
        // Could not get a resource from the pool  
        poolConfig.setMaxWaitMillis(1000);  
        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();  
        nodes.add(new HostAndPort("192.168.1.136", 7000));  
        nodes.add(new HostAndPort("192.168.1.137", 7000));  
       
        JedisCluster cluster = new JedisCluster(nodes,poolConfig);  
       // String name = cluster.get("name");  
        //System.out.println(name);  
       // cluster.del("c093a57347204e83bb8f6141e9081724_jzy_age");  
        System.out.println(cluster.get("c093a57347204e83bb8f6141e9081724_jzy_age"));  
    }  
   /* *//** 
     * 设置 String 
     * 
     * @param key 
     * @param value 
     *//*  
    public synchronized static void setString(String key, String value) {  
        try {  
            value = StringUtils.isEmpty(value) ? "" : value;  
            getJedis().set(key, value);  
        } catch (Exception e) {  
            logger.error("Set key error : " + e);  
        }  
    }  
  
    *//** 
     * 设置 过期时间 
     * 
     * @param key 
     * @param seconds 以秒为单位 
     * @param value 
     *//*  
    public synchronized static void setString(String key, int seconds, String value) {  
        try {  
            value = StringUtils.isEmpty(value) ? "" : value;  
            getJedis().setex(key, seconds, value);  
        } catch (Exception e) {  
            logger.error("Set keyex error : " + e);  
        }  
    }  
  
    *//** 
     * 获取String值 
     * 
     * @param key 
     * @return value 
     *//*  
    public synchronized static String getString(String key) {  
        if (getJedis() == null || !getJedis().exists(key)) {  
            return null;  
        }  
        return getJedis().get(key);  
    }  */
}  
