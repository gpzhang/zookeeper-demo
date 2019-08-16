import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author haishen
 * @date 2019/7/27
 * Zookeeper 有 watch 事件，
 * 是一次性触发的，
 * 当 watch 监视的数据发生变化时，
 * 通知设置了该 watch 的 client，
 * 即 watcher.
 * <p>
 * Watcher接口：
 * 定义了时间通知的相关逻辑：通知状态KeeperState、
 * 事件类型EventType、
 * 回调方法：process（WatchedEvent event）。
 * <p>
 * ZooKeeper的watcher机制，可以概括为以下三个过程：
 * 客户单注册Watcher、
 * 服务端处理Watcher、
 * 和客户端回调Watcher
 */
public class WatcherImpl implements Watcher {

    /**
     * 参数WatchedEvent包含了事件的三个基本属性：
     * 通知状态（keeperState）、
     * 事件类型（EventType）、
     * 节点路径（path）
     *
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        //影响的路径
        String path = event.getPath();
        //获取事件的状态
        Event.KeeperState state = event.getState();
        //获取事件的类型
        Event.EventType type = event.getType();

        System.out.println("节点路径:" + path);
        if (Event.KeeperState.SyncConnected.equals(state)) {
            if (Event.EventType.None.equals(type)) {
                //连接建立成功，则释放信号量，让阻塞的程序继续向下执行
                System.out.println("zk建立连接成功========");
            } else if (Event.EventType.NodeCreated.equals(type)) {
                System.out.println("创建节点通知");
            } else if (Event.EventType.NodeDataChanged.equals(type)) {
                System.out.println("修改节点数据通知");
            } else if (Event.EventType.NodeDeleted.equals(type)) {
                System.out.println("删除节点通知");
            } else if (Event.EventType.NodeChildrenChanged.equals("")) {
                System.out.println("子节点变动通知");
            }
        } else if (Event.KeeperState.Disconnected.equals(state)) {
            System.out.println("与ZK服务器断开连接");
        } else if (Event.KeeperState.AuthFailed.equals(state)) {
            System.out.println("权限检查失败");
        } else if (Event.KeeperState.Expired.equals(state)) {
            System.out.println("会话失效");
        }
    }
}
