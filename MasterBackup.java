package MasterBackup;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

/**
 * 演示怎样利用zk做HA
 */
public class MasterBackup {

    // zookeeper的服务器地址IP以及端口号
    static String ZkConnectStr="127.0.0.1:2181";
    // 注册结点的路径
    static String ZkPath="/root/master";
    ZkClient client=new ZkClient(ZkConnectStr,5000);

    Object lock=new Object();

    /**
     * 需要做高可用性配置的业务方法
     */
    public void schedule() {

        // 标记位用来指示是否是master结点
        boolean isMaster=false;
        client.deleteRecursive("/root");
        if(!client.exists("/root")){
            System.out.println("root has not been created");
            client.createPersistent("/root");
        }
        try{
            client.createEphemeral(ZkPath);
            // 说明注册成功
            isMaster=true;
        }catch (Exception ex){
            // 说明注册失败，master结点已经注册了，该进程作为backup运行
            System.out.println("master has already been registed");
        }

        if(!isMaster){
            // 作为backup结点，需要注册一个监听器
            client.subscribeDataChanges(ZkPath, new IZkDataListener() {
                @Override
                public void handleDataChange(String s, Object o) throws Exception {

                }
                @Override
                public void handleDataDeleted(String s) throws Exception {
                    // master结点被删除, 说明master已经宕机, backup结点被唤醒
                    lock.notifyAll();
                }
            });
            // backup结点阻塞，直到被唤醒
            synchronized (lock){
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // 现在backup结点已经被唤醒, 开始进入正常处理流程
            isMaster=true;
        }

        while (true){
            System.out.println("Do working......");
            try {
                Thread.sleep(1000);
            }catch (InterruptedException ex){

            }

        }
    }

    public static void main(String[] args) {
        MasterBackup masterBackup=new MasterBackup();
        masterBackup.schedule();
    }
}
