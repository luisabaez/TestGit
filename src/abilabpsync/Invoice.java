/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package abilabpsync;

import java.util.Calendar;
import java.util.Date;
import sun.util.calendar.CalendarDate;

/**
 *
 * @author LuisA
 */
public class Invoice {
    public String invoiceDate;
    public String dueDate;
    public String id;
    public String referenceNumber;
    public String customerID;
    public String billingAddress1;
    public String billingAddress2;
    public String billingAddress3;
    public String billingCity;
    public String billingState;
    public String billingZip;
    public String lineItemQty;
    public String lineItemPrice;
    public String lineItemTotal;
    public String lineItemDescription;

    public Invoice(String invoiceDate, String dueDate, String id, String referenceNumber, String customerID, String billingAddress1, String billingAddress2, String billingAddress3, String billingCity, String billingState, String billingZip, String lineItemQty, String lineItemPrice, String lineItemTotal, String lineItemDescription) {
        this.invoiceDate = invoiceDate;
        this.dueDate = dueDate;
        this.id = id;
        this.referenceNumber = referenceNumber;
        this.customerID = customerID;
        this.billingAddress1 = billingAddress1;
        this.billingAddress2 = billingAddress2;
        this.billingAddress3 = billingAddress3;
        this.billingCity = billingCity;
        this.billingState = billingState;
        this.billingZip = billingZip;
        this.lineItemQty = lineItemQty;
        this.lineItemPrice = lineItemPrice;
        this.lineItemTotal = lineItemTotal;
        this.lineItemDescription = lineItemDescription;
    }

    public Invoice(String id, String referenceNumber, String customerID, String lineItemTotal) {
        this.id = id;
        this.referenceNumber = referenceNumber;
        this.customerID = customerID;
        this.lineItemTotal = lineItemTotal;
    }
    
    public Invoice(String id, String referenceNumber, String lineItemTotal) {
        this.id = id;
        this.referenceNumber = referenceNumber;
        this.lineItemTotal = lineItemTotal;
    }

    public Invoice(String id, String referenceNumber) {
        this.id = id;
        this.referenceNumber = referenceNumber;
    }  
    
    public Invoice(String id) {
        this.id = id;
    } 

    public String getInvoiceDate() {
        return invoiceDate;
    }

    public String getDueDate() {
        return dueDate;
    }

    public String getId() {
        return id;
    }

    public String getReferenceNumber() {
        return referenceNumber;
    }

    public String getCustomerID() {
        return customerID;
    }

    public String getBillingAddress1() {
        return billingAddress1;
    }

    public String getBillingAddress2() {
        return billingAddress2;
    }

    public String getBillingAddress3() {
        return billingAddress3;
    }

    public String getBillingCity() {
        return billingCity;
    }

    public String getBillingState() {
        return billingState;
    }

    public String getBillingZip() {
        return billingZip;
    }

    public String getLineItemQty() {
        return lineItemQty;
    }

    public String getLineItemPrice() {
        return lineItemPrice;
    }

    public String getLineItemTotal() {
        return lineItemTotal;
    }

    public String getLineItemDescription() {
        return lineItemDescription;
    }

    public void setReferenceNumber(String referenceNumber) {
        this.referenceNumber = referenceNumber;
    }    

    public void setCustomerID(String customerID) {
        this.customerID = customerID;
    }
    
    
    
}


