import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author haishen
 * @date 2019/7/26
 * ZAB协议三个阶段:
 * 1.发现(选举Leader过程)
 * 2.同步(选出Leader后，Follower和Observer需进行数据同步)
 * 3.广播(同步之后，集群对外工作响应请求，并进行消息广播，实现数据在集群节点的副本存储)
 * <p>
 * 监听节点是否存在，存在就通知
 * 监听节点的存储信息是否变化，变化即通知
 * 监听子节点列表，有变化即通知
 */
public class Main {

    private static String rootNode = "/study";
    private static String childrenNode = rootNode + "/0727";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        ZookeeperBaseApi base = new ZookeeperBaseApi("localhost:2181", 15000);

        //建立连接
        ZooKeeper zkClient = base.getZkClient();
        System.out.println("sessionId:" + zkClient.getSessionId() + ",sessionTimeOut:" + zkClient.getSessionTimeout());

        //判断节点是否存在，添加监听
        if (base.exists(rootNode) == null) {
            //创建父节点
            base.createNode(rootNode, "节点" + rootNode + "数据创建");
        }
        if (base.exists(childrenNode) == null) {
            //创建子节点
            base.createNode(childrenNode, "节点" + rootNode + "的子节点" + childrenNode + "数据创建");
        }

        //获取节点数据，添加监听
        base.getData(rootNode);
        //修改节点信息
        base.updateNodeData(rootNode, "节点" + rootNode + "数据修改");

        //获取子节点数据
        base.getData(childrenNode);
        //修改节点信息
        base.updateNodeData(childrenNode, "节点" + rootNode + "的子节点" + childrenNode + "数据修改");

        //获取节点的子节点列表，添加监听
        base.getChildren(childrenNode);
        //删除节点
        base.deleteNode(childrenNode);

//        base.deleteNode(rootNode);

        //关闭连接
        base.closeClient();
    }
}
