package com.hong.py.pojo;

import java.io.Serializable;

public class User  implements Serializable {

    private Integer userid;
    private String userName;

    public Integer getUserid() {
        return userid;
    }

    public void setUserid(Integer userid) {
        this.userid = userid;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String username) {
        this.userName = username;
    }
}
