/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package abilabpsync;

import java.util.ArrayList;

/**
 *
 * @author Luis A. Baez-Black
 */
public class InvoiceBalanceDetail {
    private String id;
    private String invRefNo;
    private String sessionID;
    private String transSource;
    private String cusID;
    private String glBalance; 
    private ArrayList<AccountCode> accountDistribution = new ArrayList<AccountCode>();
    private String sCodeIDf_0;
    private String sCodeIDf_1;
    private String sCodeIDf_2;
    private String sCodeIDf_3;
    private String sCodeIDf_4;
    private String sCodeIDf_5;
    private String sCodeIDf_6;
    private String sCodeIDf_7;
    private String offsetGL;
    private String offsetFund;
    private String offsetIF;
    private String triggerIF;

    public InvoiceBalanceDetail(String id, String invRefNo, String sessionID, String transSource, String cusID, String glBalance, ArrayList<AccountCode> accountDistribution, String sCodeIDf_0, String sCodeIDf_1, String sCodeIDf_2, String sCodeIDf_3, String sCodeIDf_4, String sCodeIDf_5, String sCodeIDf_6, String sCodeIDf_7, String offsetGL, String offsetFund, String offsetIF, String triggerIF) {
        this.id = id;
        this.invRefNo = invRefNo;
        this.sessionID = sessionID;
        this.transSource = transSource;
        this.cusID = cusID;
        this.glBalance = glBalance;
        this.accountDistribution = accountDistribution;
        this.sCodeIDf_0 = sCodeIDf_0;
        this.sCodeIDf_1 = sCodeIDf_1;
        this.sCodeIDf_2 = sCodeIDf_2;
        this.sCodeIDf_3 = sCodeIDf_3;
        this.sCodeIDf_4 = sCodeIDf_4;
        this.sCodeIDf_5 = sCodeIDf_5;
        this.sCodeIDf_6 = sCodeIDf_6;
        this.sCodeIDf_7 = sCodeIDf_7;
        this.offsetGL = offsetGL;
        this.offsetFund = offsetFund;
        this.offsetIF = offsetIF;
        this.triggerIF = triggerIF;
    }

    public String getId() {
        return id;
    }

    public String getInvRefNo() {
        return invRefNo;
    }

    public String getSessionID() {
        return sessionID;
    }

    public String getTransSource() {
        return transSource;
    }
    
        public String getCusID() {
        return cusID;
    }

    public String getGlBalance() {
        return glBalance;
    }

    public ArrayList<AccountCode> getAccountDistribution() {
        return accountDistribution;
    }
    
    public String getsCodeIDf_0() {
        return sCodeIDf_0;
    }

    public String getsCodeIDf_1() {
        return sCodeIDf_1;
    }

    public String getsCodeIDf_2() {
        return sCodeIDf_2;
    }

    public String getsCodeIDf_3() {
        return sCodeIDf_3;
    }

    public String getsCodeIDf_4() {
        return sCodeIDf_4;
    }

    public String getsCodeIDf_5() {
        return sCodeIDf_5;
    }

    public String getsCodeIDf_6() {
        return sCodeIDf_6;
    }

    public String getsCodeIDf_7() {
        return sCodeIDf_7;
    }

    public String getOffsetGL() {
        return offsetGL;
    }

    public String getOffsetFund() {
        return offsetFund;
    }

    public String getOffsetIF() {
        return offsetIF;
    }

    public String getTriggerIF() {
        return triggerIF;
    }

    public void setAccountDistribution(ArrayList<AccountCode> accountDistribution) {
        this.accountDistribution = accountDistribution;
    }
    
    public void addAccountCode(AccountCode ac){
        this.accountDistribution.add(ac);
    }
    
}
