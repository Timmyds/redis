package com.jzy.redis.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

/**
 * 在集群模式下提供批量操作的功能。 <br/>
 * 由于集群模式存在节点的动态添加删除，且client不能实时感知（只有在执行命令时才可能知道集群发生变更），
 * 因此，该实现不保证一定成功，建议在批量操作之前调用 refreshCluster() 方法重新获取集群信息。<br />
 * 应用需要保证不论成功还是失败都会调用close() 方法，否则可能会造成泄露。<br/>
 * 如果失败需要应用自己去重试，因此每个批次执行的命令数量需要控制。防止失败后重试的数量过多。<br />
 * 基于以上说明，建议在集群环境较稳定（增减节点不会过于频繁）的情况下使用，且允许失败或有对应的重试策略。<br />
 * 
 * 
 * @version
 * @since Ver 1.1
 */
public class JedisClusterUtil extends PipelineBase implements Closeable {

 
    // 部分字段没有对应的获取方法，只能采用反射来做
    // 你也可以去继承JedisCluster和JedisSlotBasedConnectionHandler来提供访问接口
    private static final Field FIELD_CONNECTION_HANDLER;
    private static final Field FIELD_CACHE; 
    static {
        FIELD_CONNECTION_HANDLER = getField(BinaryJedisCluster.class, "connectionHandler");
        FIELD_CACHE = getField(JedisClusterConnectionHandler.class, "cache");
    }

    private JedisSlotBasedConnectionHandler connectionHandler;
    private JedisClusterInfoCache clusterInfoCache;
    private Queue<Client> clients = new LinkedList<Client>();   // 根据顺序存储每个命令对应的Client
    private Map<JedisPool, Jedis> jedisMap = new HashMap<JedisPool, Jedis>();   // 用于缓存连接
    private boolean hasDataInBuf = false;   // 是否有数据在缓存区

    
    /**
	 * 客户端标识（设定后不可改变）
	 */
	private String CLIENT_ID;

	
    /**
     * 根据jedisCluster实例生成对应的JedisClusterPipeline
     * @param 
     * @return
     */
    public static JedisClusterUtil getInstance(Map<String, Integer> addArray, String CLIENT_ID) {
    	JedisPoolConfig poolConfig = new JedisPoolConfig();
		// 最大连接数
		poolConfig.setMaxTotal(100);
		// 最大空闲数
		poolConfig.setMaxIdle(10);
		// 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
		poolConfig.setMaxWaitMillis(1000);
		Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
		for (Entry<String, Integer> entry : addArray.entrySet()) {
			nodes.add(new HostAndPort((String) entry.getKey(), (Integer) entry.getValue()));
		}
		JedisCluster jedisCluster = new JedisCluster(nodes, poolConfig);
        JedisClusterUtil pipeline = new JedisClusterUtil();
        pipeline.setJedisCluster(jedisCluster);
        pipeline.CLIENT_ID = CLIENT_ID;
        return pipeline;
    }

    public JedisClusterUtil() {
    }

    public void setJedisCluster(JedisCluster jedis) {
        connectionHandler = getValue(jedis, FIELD_CONNECTION_HANDLER);
        clusterInfoCache = getValue(connectionHandler, FIELD_CACHE);
    }

    /**
     * 刷新集群信息，当集群信息发生变更时调用
     * @param 
     * @return
     */
    public void refreshCluster() {
        connectionHandler.renewSlotCache();
    }



    public void close() {
        clean();

        clients.clear();

        for (Jedis jedis : jedisMap.values()) {
            if (hasDataInBuf) {
                flushCachedData(jedis);
            }

            jedis.close();
        }

        jedisMap.clear();

        hasDataInBuf = false;
    }

    private void flushCachedData(Jedis jedis) {
        try {
            jedis.getClient().getAll();
        } catch (RuntimeException ex) {
        }
    }

    @Override
    protected Client getClient(String key) {
        byte[] bKey = SafeEncoder.encode(key);

        return getClient(bKey);
    }

    @Override
    protected Client getClient(byte[] key) {
        Jedis jedis = getJedis(JedisClusterCRC16.getSlot(key));

        Client client = jedis.getClient();
        clients.add(client);

        return client;
    }

    private Jedis getJedis(int slot) {
        JedisPool pool = clusterInfoCache.getSlotPool(slot);

        // 根据pool从缓存中获取Jedis
        Jedis jedis = jedisMap.get(pool);
        if (null == jedis) {
            jedis = pool.getResource();
            jedisMap.put(pool, jedis);
        }

        hasDataInBuf = true;
        return jedis;
    }

    private static Field getField(Class<?> cls, String fieldName) {
        try {
            Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);

            return field;
        } catch (Exception  e) {
            throw new RuntimeException("cannot find or access field '" + fieldName + "' from " + cls.getName(), e);
        }
    }

    @SuppressWarnings({"unchecked" })
    private static <T> T getValue(Object obj, Field field) {
        try {
            return (T)field.get(obj);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }   

    public static void main(String[] args) throws IOException {
   
        String str="c093a57347204e83bb8f6141e9081724_jzy_";
        Map<String,Integer> addArray=new  HashMap();
        addArray.put("192.168.1.136", 7000);
        addArray.put("192.168.1.137", 7000);
       
        long s = System.currentTimeMillis();

        JedisClusterUtil jcp = JedisClusterUtil.getInstance(addArray, str);
        jcp.refreshCluster();
        List<Object> batchResult = null;
        try {
            // batch write
            for (int i = 0; i < 10000; i++) {
                jcp.set("c093a57347204e83bb8f6141e9081724_jzy_k" + i, "v1" + i);
            }
          // jcp.sync();

            // batch read
            for (int i = 0; i < 10000; i++) {
                jcp.get("c093a57347204e83bb8f6141e9081724_jzy_k" + i);
            }
            //batchResult = jcp.syncAndReturnAll();
        } finally {
            jcp.close();
        }

        // output time 
        long t = System.currentTimeMillis() - s;
        System.out.println(t);

        //System.out.println(batchResult.size());

        
    }
}