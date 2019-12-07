package com.hong.py;

import com.hong.py.pojo.User;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientCrudTest {

    final static Logger logger = LoggerFactory.getLogger(ZkClientCrudTest.class);

    public static void main(String[] args) {
        ZkClientCrud<User> zkClientCrud=new ZkClientCrud<User>(new SerializableSerializer());
        String path="/root";
        zkClientCrud.deleteRecursive(path); //递归删除
        zkClientCrud.createPersistent(path,"hi");

     /*  zkClientCrud.createPersistent(path+"/a/b/c",true);//递归创建 但是不能设在value
       //zkClientCrud.createPersistent(path,"hi");
        logger.info(zkClientCrud.readData(path));
        //更新
        zkClientCrud.writeData(path,"hello");
        logger.info(zkClientCrud.readData(path));
        logger.info(String.valueOf(zkClientCrud.getChildren(path)));
        //子节点
        List<String> list=zkClientCrud.getChildren(path);
        for(String child:list){
            logger.info("子节点:"+child);
        }*/

        User user=new User();
        user.setUserid(1);
        user.setUserName("张三");
        zkClientCrud.writeData(path,user);
        System.out.println(zkClientCrud.readData(path).getUserName());


    }
}
