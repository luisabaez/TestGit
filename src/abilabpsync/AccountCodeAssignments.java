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
public class AccountCodeAssignments {

    public AccountCodeAssignments(String glCode, ArrayList<Integer> reqSeg) {
        this.glCode = glCode;
        this.reqSegments = reqSeg;        
    }

    private String glCode;
    private ArrayList<Integer> reqSegments = new ArrayList<Integer>();    
    
    public String getGlCode() {
        return glCode;
    }

    public void setGlCode(String glCode) {
        this.glCode = glCode;
    }
    
    public void clearAssignments(){
        this.reqSegments.clear();
    }
    
    public Integer getReqSegmentID(Integer index) {
        return this.reqSegments.get(index);
    }

    public void setReqSegments(Integer segmentID) {
        this.reqSegments.add(segmentID);        
    }

    public ArrayList<Integer> getReqSegments() {
        return reqSegments;
    }
    
    
}

