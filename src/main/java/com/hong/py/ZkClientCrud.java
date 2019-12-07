package com.hong.py;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * CURD操作
 * @param <T>
 */
public class ZkClientCrud<T> {

    ZkClient zkClient ;
    final static Logger logger = LoggerFactory.getLogger(ZkClientCrud.class);

    public ZkClientCrud(ZkSerializer zkSerializer) {
        logger.info("链接zk开始");
        // zkClient=new ZkClient(ZookeeperUtil.connectString,ZookeeperUtil.sessionTimeout);
        zkClient=new ZkClient(ZookeeperUtil.connectString,ZookeeperUtil.sessionTimeout,ZookeeperUtil.sessionTimeout,zkSerializer);
    }


    public void createEphemeral(String path,Object data){

        zkClient.createEphemeral(path,data);
    }

    /***
     * 支持创建递归方式
     * @param path
     * @param createParents
     */
    public void createPersistent(String path,boolean createParents){

        zkClient.createPersistent(path,createParents);
    }

    /***
     * 创建节点 跟上data数据
     * @param path
     * @param data
     */
    public void createPersistent(String path,Object data){

        zkClient.createPersistent(path,data);
    }

    /***
     * 子节点
     * @param path
     * @return
     */
    public List<String> getChildren(String path){
        return zkClient.getChildren(path);

    }

    public  T readData(String path){
        return zkClient.readData(path);
    }

    public  void  writeData(String path,Object data){
        zkClient.writeData(path,data);
    }

    //递归删除
    public  void deleteRecursive(String path){
        zkClient.deleteRecursive(path);

    }
}
