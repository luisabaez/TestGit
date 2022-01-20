/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package abilabpsync;

import java.util.ArrayList;

/**
 *
 * @author LuisA
 */
public class Payment {
    private String pmtDate;
    private String id;
    private String pmtMethod;
    private String pmtSessionNum;
    private String pmtAmount;
    private String customerNo;
    private String customerName;
    private String referenceNo;
    private String fund;    
    private ArrayList<Invoice> appliedTo;
    private ArrayList<String> appliedAmount;
    private String lastUpdate;

    public Payment(String pmtDate, String pmtMethod, String pmtAmount, String customerNo, String customerName, String referenceNo, ArrayList<Invoice> appliedTo, ArrayList<String> appliedAmount, String lastUpdate) {
        this.pmtDate = pmtDate;
        this.pmtMethod = pmtMethod;
        this.pmtAmount = pmtAmount;
        this.customerNo = customerNo;
        this.customerName = customerName;
        this.referenceNo = referenceNo;
        this.appliedTo = appliedTo;
        this.appliedAmount = appliedAmount;
        this.lastUpdate = lastUpdate;
    }  
    
    public Payment(String pmtDate, String id, String pmtAmount, String customerNo, ArrayList<Invoice> appliedTo, ArrayList<String> appliedAmount) {
        this.pmtDate = pmtDate;
        this.id = id;
        this.pmtAmount = pmtAmount;
        this.customerNo = customerNo;
        this.appliedTo = appliedTo;
        this.appliedAmount = appliedAmount;
    }

    public Payment(ArrayList<Invoice> appliedTo, ArrayList<String> appliedAmount, String pmtDate, String lastUpdate, String id, String pmtAmount, String customerNo, String referenceNo, String fund, String method, String session) {
        this.pmtDate = pmtDate;
        this.lastUpdate = lastUpdate;
        this.id = id;
        this.pmtAmount = pmtAmount;
        this.customerNo = customerNo;
        this.referenceNo = referenceNo;
        this.appliedTo = appliedTo;
        this.appliedAmount = appliedAmount;
        this.fund = fund;
        this.pmtMethod = method;
        this.pmtSessionNum = session;      
    }

    public Payment(String referenceNo, String customerNo, String pmtAmt, String pmtDate, String lastUpdate) {
        this.referenceNo = referenceNo;
        this.customerNo = customerNo;
        this.pmtAmount = pmtAmt;
        this.pmtDate = pmtDate;
        this.lastUpdate = lastUpdate;
    }
    
    public Payment() {
        this.pmtDate = "";
        this.id = "";
        this.pmtMethod = "";
        this.pmtAmount = "";
        this.customerNo = "";
        this.appliedTo = null;
        this.appliedAmount = null;
        this.fund = "";
        this.pmtMethod = "";
    }

    public String getId() {
        return id;
    }    
       
    public String getReferenceNo() {
        return referenceNo;
    }

    public String getCustomerNo() {
        return customerNo;
    }

    public String getCustomerName() {
        return customerName;
    }
    
    public ArrayList<Invoice> getAppliedTo() {
        return appliedTo;
    }

    public ArrayList<String> getAppliedAmount() {
        return appliedAmount;
    }

    public String getPmtDate() {
        return pmtDate;
    }

    public String getPmtAmount() {
        return pmtAmount;
    }
    
    public String getPmtMethod() {
        return pmtMethod;
    }
    
    public String getPmtSessionNum(){
        return pmtSessionNum;
    }

    public String getFund() {
        return fund;
    }

    public String getLastUpdate() {
        return lastUpdate;
    }
    
    
    
    public void setPmtDate(String pmtDate) {
        this.pmtDate = pmtDate;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPmtMethod(String pmtMethod) {
        this.pmtMethod = pmtMethod;
    }

    public void setPmtAmount(String pmtAmount) {
        this.pmtAmount = pmtAmount;
    }

    public void setCustomerNo(String customerNo) {
        this.customerNo = customerNo;
    }

    public void setAppliedTo(ArrayList<Invoice> appliedTo) {
        this.appliedTo = appliedTo;
    }

    public void setAppliedAmount(ArrayList<String> appliedAmount) {
        this.appliedAmount = appliedAmount;
    }
    
    
    
}


