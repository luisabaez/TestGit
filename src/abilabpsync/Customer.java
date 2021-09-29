/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package abilabpsync;

/**
 *
 * @author LuisA
 */
public class Customer {
    
    private String id = "";
    private String companyName = "";
    private String firstName = "";
    private String middleInitial = "";
    private String lastName = "";
    private String status = "";    
    private String email = "";
    private String fullName = "";

    public Customer() {
        this.id = "";
        this.companyName = "";
        this.firstName = "";
        this.middleInitial = "";
        this.lastName = "";
        this.status = "";    
        this.email = "";
        this.fullName = "";
    }

    
    
    public Customer(String id, String c, String fn, String mi, String ln, String s, String e) {
        this.id = id;
        this.companyName = c;
        this.firstName = fn;
        this.middleInitial = mi;
        this.lastName = ln;
        this.status = s;    
        this.email = e;
        if(mi==null){
            this.fullName = fn+" "+ln;
        }else if(mi.contains(" ")){
            this.fullName = fn+" "+ln;        
        }else{
            this.fullName = fn+" "+mi+" "+ln;
        }
        
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getMiddleInitial() {
        return middleInitial;
    }

    public void setMiddleInitial(String middleInitial) {
        this.middleInitial = middleInitial;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFullName(){
        return fullName;
    }
    
    public void setFullName(String fullName){
        this.fullName = fullName;
    }

    @Override
  public String toString() {
    return this.id +" - "+ this.companyName;
  }
    
    

     
   
    
}
