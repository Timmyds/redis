package com.jzy.redis.util;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis 工具类
 */
public class RedisUtil {

	private JedisCluster jedisCluster;
	/**
	 * 客户端标识（设定后不可改变）
	 */
	private String CLIENT_ID;

	public RedisUtil(Map<String, Integer> addArray, String CLIENT_ID) {
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
		this.jedisCluster = new JedisCluster(nodes, poolConfig);
		this.CLIENT_ID = CLIENT_ID;
	}

	/**
	 * 通过key删除
	 * 
	 * @param key
	 */
	public long del(final String... keys) {

		long result = 0;
		for (int i = 0; i < keys.length; i++) {
			result = jedisCluster.del((CLIENT_ID + keys[i]).getBytes());
		}
		return result;

	}

	/**
	 * 通过key删除
	 * 
	 * @param key
	 */
	public void delete(final String... keys) {
		jedisCluster.del(CLIENT_ID + keys);
	}

	/**
	 * @param key
	 * @return
	 */
	public String type(final String key) {
		return jedisCluster.type(CLIENT_ID + key);
	}

	/**
	 * 添加key value 并且设置存活时间(byte)
	 * 
	 * @param key
	 * @param value
	 * @param liveTime
	 */
	public void set(final byte[] key, final byte[] value, final long liveTime) {

		jedisCluster.set((CLIENT_ID+key).getBytes(), value);
		if (liveTime > 0) {
			jedisCluster.pexpire(CLIENT_ID +key, liveTime);
		}

	}

	public Long setnx(final byte[] key, final byte[] value, final long liveTime) {

		Long flag = jedisCluster.setnx((CLIENT_ID +key).getBytes(), value);
		if (flag > 0 && liveTime > 0) {
			jedisCluster.pexpire(CLIENT_ID +key, liveTime);
		}
		return flag;
	}

	/**
	 * @param key
	 * @param value
	 * @param liveTime
	 */
	public Long setNX(String key, String value, long liveTime) {
		return this.setnx(key.getBytes(), value.getBytes(), liveTime);
	}

	/**
	 * 添加key value 并且设置存活时间
	 * 
	 * @param key
	 * @param value
	 * @param liveTime
	 *            单位秒
	 */
	public void set(String key, String value, long liveTime) {
		this.set(key.getBytes(), value.getBytes(), liveTime);
	}

	/**
	 * set 新增
	 * 
	 * @param key
	 * @param value
	 * @param liveTime
	 *            有效时间 单位（秒）
	 */
	public void setObject(final String key, final byte[] value, final long liveTime) {
		this.set(key.getBytes(), value, liveTime);
	}

	/**
	 * 获取redis value (String)
	 * 
	 * @param key
	 * @return
	 */
	public byte[] get(final String key) {
		return jedisCluster.get((CLIENT_ID +key).getBytes());
	}

	/**
	 * 获取redis value (String)
	 * 
	 * @param key
	 * @return
	 */
	public String getString(final String key) {
		byte[] bytes = this.get(key);
		if (bytes != null) {
			return new String(this.get(key));
		} else {
			return null;
		}
	}

	/**
	 * 根据Key
	 * 
	 * @param key
	 * @param liveTime
	 *            有效时间 单位（秒）
	 */
	public void expire(final String key, final long liveTime) {

		jedisCluster.pexpire((CLIENT_ID +key).getBytes(), liveTime);

	}

	/**
	 * 使用set时，检查key是否已经存在
	 * 
	 * @param key
	 * @return true 存在，false 不存在
	 */
	public boolean exists(final String key) {

		return jedisCluster.exists(CLIENT_ID +key);

	}

	public void close() {
		try {
			jedisCluster.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 设置 String
	 * 
	 * @param key
	 * @param value
	 */
	public synchronized void setString(String key, String value) {
		if (jedisCluster != null) {
			value = StringUtils.isEmpty(value) ? "" : value;
			jedisCluster.set(CLIENT_ID + key, value);
		}
	}

	/**
	 * 设置 过期时间
	 * 
	 * @param key
	 * @param seconds
	 *            以秒为单位
	 * @param value
	 */
	public synchronized void setString(String key, int seconds, String value) {
		if (jedisCluster != null) {
			value = StringUtils.isEmpty(value) ? "" : value;
			jedisCluster.setex(CLIENT_ID + key, seconds, value);

		}
	}



}
