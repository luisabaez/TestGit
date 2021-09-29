/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package abilabpsync;

/**
 *
 * @author Luis A. Baez-Black
 */
public class AccountCode {
    private String segmentID;
    private String codeID;

    public AccountCode(String segmentID, String codeID) {
        this.segmentID = segmentID;
        this.codeID = codeID;
    }

    public String getSegmentID() {
        return segmentID;
    }

    public String getCodeID() {
        return codeID;
    }
    
    
    
}
