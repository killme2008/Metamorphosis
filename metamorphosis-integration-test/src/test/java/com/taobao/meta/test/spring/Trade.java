package com.taobao.meta.test.spring;

import java.io.Serializable;


/**
 * A trade
 * 
 * @author dennis
 * 
 */
public class Trade implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private long id;
    private String name;
    private int money;
    private String address;


    public Trade() {
        super();
    }


    public Trade(long id, String name, int money, String address) {
        super();
        this.id = id;
        this.name = name;
        this.money = money;
        this.address = address;
    }


    public long getId() {
        return this.id;
    }


    public void setId(long id) {
        this.id = id;
    }


    public String getName() {
        return this.name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public int getMoney() {
        return this.money;
    }


    public void setMoney(int money) {
        this.money = money;
    }


    public String getAddress() {
        return this.address;
    }


    public void setAddress(String address) {
        this.address = address;
    }


    @Override
    public String toString() {
        return "Trade [id=" + this.id + ", name=" + this.name + ", money=" + this.money + ", address=" + this.address
                + "]";
    }

}
