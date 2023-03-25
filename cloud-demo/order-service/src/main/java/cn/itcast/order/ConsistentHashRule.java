package cn.itcast.order;

import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.AbstractLoadBalancerRule;
import com.netflix.loadbalancer.Server;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRule extends AbstractLoadBalancerRule {

    // 定义虚拟节点数
    private static final int VIRTUAL_NODE_NUM = 3;

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
    }

    @Override
    public Server choose(Object key) {
        List<Server> servers = getLoadBalancer().getAllServers();

        // 构建虚拟节点哈希环
        TreeMap<Long, Server> consistentHashMap = new TreeMap<>();
        for (Server server : servers) {
            for (int i = 0; i < VIRTUAL_NODE_NUM; i++) {
                String virtualNodeName = server.getId() + "-VN" + i;
                consistentHashMap.put((long) virtualNodeName.hashCode(), server);
            }
        }

        // 根据请求的key计算其哈希值，找到最近的节点
        long hash = getHash(key.toString());
        SortedMap<Long, Server> tailMap = consistentHashMap.tailMap(hash);
        if (tailMap.isEmpty()) {
            hash = consistentHashMap.firstKey();
        } else {
            hash = tailMap.firstKey();
        }
        return consistentHashMap.get(hash);
    }

    /**
     * 计算字符串的哈希值
     */
    private long getHash(String key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < key.length(); i++)
            hash = (hash ^ key.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash & 0x7FFFFFFF;
    }
}