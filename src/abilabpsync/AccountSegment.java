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
public class AccountSegment {
    String title;
    String id;
    String length;
    String type;

    public AccountSegment(String title, String id, String length, String type) {
        this.title = title;
        this.id = id;
        this.length = length;
        this.type = type;
    }

    public String getTitle() {
        return title;
    }

    public String getId() {
        return id;
    }

    public String getLength() {
        return length;
    }

    public String getType() {
        return type;
    }
    
    
    
    
}
