import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 原生API操作zk
 *
 * @author haishen
 * @date 2019/7/27
 */

public class ZookeeperBaseApi {

    private ZooKeeper zookeeper;

    /**
     * 连接zk服务端的ip:port,如果多个格式为 ip:port,ip:port......
     */
    private String connectString;

    /**
     * 客户端连接超时时间 单位 ms
     */
    private int sessionTimeout;

    /**
     * 信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号
     */
    private CountDownLatch countDown = new CountDownLatch(1);


    public ZookeeperBaseApi(String connectString, int sessionTimeout) throws IOException {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeout;
        zookeeper = new ZooKeeper(this.connectString, this.sessionTimeout, new MyWatcher());
    }


    public ZooKeeper getZkClient() throws InterruptedException {
        //计数器到达0之前，一直阻塞，只有当信号量被释放，才会继续向下执行
        countDown.await();
        return zookeeper;
    }


    public void closeClient() throws InterruptedException {
        if (zookeeper != null) {
            zookeeper.close();
            System.out.println("zk关闭连接成功=======");
        }
    }

    /**
     * 创建节点
     * 当前节点的父节点必须存在，不然会创建异常
     * zk原生的api不支持递归创建
     *
     * @param path 节点path
     * @param data 节点数据
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */


    public String createNode(String path, String data) throws InterruptedException, KeeperException {
        ZooKeeper zk = this.getZkClient();
        String str = zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return str;
    }

    /**
     * 更新节点数据
     *
     * @param path 节点path
     * @param data 节点数据
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */


    public Stat updateNodeData(String path, String data) throws InterruptedException, KeeperException {
        ZooKeeper zk = this.getZkClient();
        Stat stat = zk.setData(path, data.getBytes(), -1);
        return stat;
    }

    /**
     * 获得节点数据
     *
     * @param path 节点path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */


    public String getData(String path) throws KeeperException, InterruptedException {
        ZooKeeper zk = this.getZkClient();
        //对访问的节点数据添加监听器
        byte[] data = zk.getData(path, true, null);
        String result = new String(data);
        System.out.println("节点[" + path + "]创建后数据:" + result);
        return result;
    }

    /**
     * 获取当前节点的子节点(不包含孙子节点)
     *
     * @param path 父节点path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */


    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        ZooKeeper zk = this.getZkClient();
        //对访问的节点的子节点添加监听器
        List<String> list = zk.getChildren(path, true);
        System.out.println("节点[" + path + "]的子节点列表:" + list.toString());
        return list;
    }

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */


    public Stat exists(String path) throws KeeperException, InterruptedException {
        ZooKeeper zk = this.getZkClient();
        //对访问的节点添加监听器
        Stat stat = zk.exists(path, true);
        return stat;
    }

    /**
     * 删除节点
     *
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     */


    public void deleteNode(String path) throws InterruptedException, KeeperException {
        ZooKeeper zk = this.getZkClient();
        zk.delete(path, -1);
        System.out.println("删除 " + path + " 节点成功");
    }

    /**
     * 自定义watcher监听
     */
    class MyWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            //影响的路径
            String path = event.getPath();
            System.out.println("path:" + path);
            //获取事件的状态
            Watcher.Event.KeeperState state = event.getState();
            //获取事件的类型
            Watcher.Event.EventType type = event.getType();

            if (Watcher.Event.KeeperState.SyncConnected.equals(state)) {
                if (Watcher.Event.EventType.None.equals(type)) {
                    //连接建立成功，则释放信号量，让阻塞的程序继续向下执行
                    countDown.countDown();
                    System.out.println("zk建立连接成功========");
                }
            }
        }

    }

}