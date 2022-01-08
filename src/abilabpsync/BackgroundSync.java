/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package abilabpsync;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.Collator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JButton;
import javax.swing.SwingWorker;
import javax.swing.JTextArea;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;
import java.lang.*;
import java.text.ParseException;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javafx.scene.paint.Color;
import static org.apache.commons.lang3.StringUtils.left;
import static org.apache.commons.lang3.StringUtils.length;
import static org.apache.commons.lang3.StringUtils.right;
import static org.apache.commons.lang3.StringUtils.trim;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;


/**
 *
 * @author Luis A. Baez-Black
 */
public class BackgroundSync extends SwingWorker<Integer, Integer>{
    
    PerformPostCall pc = new PerformPostCall();
    PerformPostCallGetInvoices pci = new PerformPostCallGetInvoices();
    PerformPostCallAddInvoice pcai = new PerformPostCallAddInvoice();
    PerformPostCallUpdateInvoice pcui = new PerformPostCallUpdateInvoice();
    PerformPostCallCustomerAdd pca = new PerformPostCallCustomerAdd();
    PerformPostCallUpdateCustomer pcuc = new PerformPostCallUpdateCustomer();
    PerformPostCallGetPayments pcp = new PerformPostCallGetPayments();
    PerformPostCallGetReturnedPayments pcrp = new PerformPostCallGetReturnedPayments();
    PerformPostCallGetInvoiceRefNo pcgrf = new PerformPostCallGetInvoiceRefNo();
    PerformPostCallAddPayment pcap = new PerformPostCallAddPayment();
    PerformPostCallSendEmail pcse = new PerformPostCallSendEmail();
    String DATABASE_URL = null;
    String SERVER_NAME = null;
    String DATABASE_NAME = null;
    String USER_NAME = null;
    String PASSWORD = null;
    String billerID = null;
    String billerPassword = null;
    String lastUpdate = null;
    String CheckBoxClass = null;
    String CheckBoxType = null;
    String sStatus = null;
    String sClass = null;
    String sType = null;
    public static JTextArea intermediateJTextArea;
    private final JButton syncButton;
    private final JButton cancelButton;
    String ARCSessionID = null;
    boolean returnedPaymentsSessionCreated = false;
    boolean sendEmails = false;
    boolean syncTransWithBal = false;
    public static EventLog log = new EventLog();
    private double total;
    private double value;
    private int forCount;
    boolean prevSync = true;
    String abilaVersion = null;
    String whereClause = "";
    String appendedWhereClause = "";
    String customerToSync = MainFrame.customerToSync;
    
    
    public BackgroundSync(JTextArea intermediate, JButton sync, JButton cancel, boolean emails, boolean SyncBal){
        intermediateJTextArea = intermediate;
        syncButton = sync;
        cancelButton = cancel;
        sendEmails = emails;
        //Need to change this once Abner finishes the First Sync screen.
        syncTransWithBal = SyncBal;
        log = new EventLog();
        customerToSync = MainFrame.customerToSync;
    }
    
    @Override
    protected Integer doInBackground() throws Exception{
                        
        //Step 1 - Gather Information
        if(!isCancelled()){              
            intermediateJTextArea.append("Step 1 - Gather Information...\n");
            log.writeToFile("Step 1 - Gather Information...\n");

//            try {
//                pc.performPostCall();
//            } catch (IOException ex) {
//                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
//                intermediateJTextArea.append(ex.toString() + "\n");
//                log.writeToFile(ex.toString() + "\n");
//                this.cancel(true);
//            } catch (SAXException ex) {
//                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
//                intermediateJTextArea.append(ex.toString() + "\n");
//                log.writeToFile(ex.toString() + "\n");
//                this.cancel(true);
//            } catch (ParserConfigurationException ex) {
//                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
//                intermediateJTextArea.append(ex.toString() + "\n");
//                log.writeToFile(ex.toString() + "\n");
//                this.cancel(true);
//            }
        } else {
        intermediateJTextArea.append("Sync process cancelled...\n");
        log.writeToFile("Sync process cancelled...\n");
        log.close();
        return 100;
        }
        
        
        
            
            
            
            if(!isCancelled()){  
                //log.writeToFile("Step 1 - Gather Information...\n");
                getSQLProperties();
            }else {
                intermediateJTextArea.append("Sync process cancelled...\n");
                log.writeToFile("Sync process cancelled...\n");
                log.close();
                return 100;
            }

            //intermediateJTextArea.append("Sync process cancelled...\n");
            //log.writeToFile("Sync process cancelled...\n");            
                                          
            //Step 1.1 - Get Customers        
            intermediateJTextArea.append("Step 1.1 - Get Customers...\n");
            log.writeToFile("Step 1.1 - Get Customers...\n");

            //Step 1.1.a - Get Abila Customers
            intermediateJTextArea.append("Step 1.1.a - Get Abila Customers...\n");
            log.writeToFile("Step 1.1.a - Get Abila Customers...\n");
            if(!isCancelled()){ 
                getAbilaCustomers();
            } else {
                intermediateJTextArea.append("Sync process cancelled...\n");
                log.writeToFile("Sync process cancelled...\n");
                log.close();
                return 100;
            } 
        
        //Step 1.1.b - Get BP Customers  
        
        if(!isCancelled()){  
            intermediateJTextArea.append("Step 1.1.b - Get Bill and Pay Customers...\n");
            log.writeToFile("Step 1.1.b - Get Bill and Pay Customers...\n");

            try {
                pc.performPostCall();
            } catch (IOException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (SAXException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (ParserConfigurationException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;                
        } 
        
        //Step 1.1.c - Get Abila version       
        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.1.c - Get Abila version...\n");
            log.writeToFile("Step 1.1.c - Get Abila version...\n");
            getAbilaVersion();
        }else {
        intermediateJTextArea.append("Sync process cancelled...\n");
        log.writeToFile("Sync process cancelled...\n");
        log.close();
        return 100;
        }
        
        //Step 1.1.d - Get Accountinng Segments Info        
        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.1.d - Get Accounting Segments...\n");
            log.writeToFile("Step 1.1.d - Get Accounting Segments...\n");           
            
            getAccountingSegments();
            getRequiredSegments();
        }else {
        intermediateJTextArea.append("Sync process cancelled...\n");
        log.writeToFile("Sync process cancelled...\n");
        log.close();
        return 100;
        }
                
        //Step 1.2 - Get Invoices                
        intermediateJTextArea.append("Step 1.2 - Get Invoices...\n");
        log.writeToFile("Step 1.2 - Get Invoices...\n");

        //Step 1.2.a - Get Abila Invoices
        intermediateJTextArea.append("Step 1.2.a - Get Abila Invoices...\n");
        log.writeToFile("Step 1.2.a - Get Abila Invoices...\n");
        if(!isCancelled()){  
            getAbilaInvoices();

            intermediateJTextArea.append("Retrieved " + MainFrame.abilaInvoices.size() +  " Invoice from Abila...\n");
            log.writeToFile("Retrieved " + MainFrame.abilaInvoices.size() +  " Invoice from Abila...\n");
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;               
        }        
        //Step 1.2.b - Get BP Invoices
/**        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.2.b - Get Bill and Pay Invoices...\n");
            log.writeToFile("Step 1.2.b - Get Bill and Pay Invoices...\n");
                    
            try {
                pci.performPostCall();
            } catch (IOException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (SAXException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (ParserConfigurationException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            }
        
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }**/
        
        //Step 1.3 - Get Payments
        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.3 - Get Payments...\n");
            log.writeToFile("Step 1.3 - Get Payments...\n");

            //Step 1.3.a - Get Abila Payments
            intermediateJTextArea.append("Step 1.3.a - Get Abila Payments...\n");
            log.writeToFile("Step 1.3.a - Get Abila Payments...\n");

            getAbilaPayments();
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 1.3.b - Get BP Payments
        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.3.b - Get Bill and Pay Payments...\n");
            log.writeToFile("Step 1.3.b - Get Bill and Pay Payments...\n");

            //intermediateJTextArea.append("Getting Bill and Pay Payments...\n");
            //log.writeToFile("Getting Bill and Pay Payments...\n");

            try {
                pcp.performPostCall();
            } catch (IOException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (SAXException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (ParserConfigurationException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            }

            intermediateJTextArea.append("Retrieved " + MainFrame.bpPayments.size() + " payments from Bill and Pay...\n");
            log.writeToFile("Retrieved " + MainFrame.bpPayments.size() + " payments from Bill and Pay...\n");
        
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }

        //Step 1.3.c - Get BP Returned Payments
        if(!isCancelled()){
            intermediateJTextArea.append("Step 1.3.c - Get Bill and Pay Returned Payments...\n");
            log.writeToFile("Step 1.3.b - Get Bill and Pay Returned Payments...\n");

            try {
                pcrp.performPostCall();
            } catch (IOException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (SAXException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            } catch (ParserConfigurationException ex) {
                Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                intermediateJTextArea.append(ex.toString() + "\n");
                log.writeToFile(ex.toString() + "\n");
                this.cancel(true);
            }

            intermediateJTextArea.append("Retrieved " + MainFrame.returnedPayments.size() + " Returned Payments from Bill and Pay...\n");
            log.writeToFile("Retrieved " + MainFrame.returnedPayments.size() + " Returned Payments from Bill and Pay...\n");
        
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 2 - Compare Information
        if(!isCancelled()){
        intermediateJTextArea.append("Step 2 - Compare Information...\n");
        log.writeToFile("Step 2 - Compare Information...\n");
        
        //Step 2.1 - Compare Customers
        intermediateJTextArea.append("Step 2.1 - Compare Customers...\n");
        log.writeToFile("Step 2.1 - Compare Customers...\n");
        
        orderCustomers(MainFrame.bpCustomers);
        
        compareCustomers(MainFrame.abilaCutomers, MainFrame.bpCustomers);
        
        MainFrame.progressMonitor.append("A total of " + MainFrame.customersToAdd.size() + " customers need to be added to Bill and Pay...\n");
        BackgroundSync.log.writeToFile("A total of " + MainFrame.customersToAdd.size() + " customers need to be added to Bill and Pay...\n");

        MainFrame.progressMonitor.append("A total of " + MainFrame.customersToUpdate.size() + " customers need to be updated to Bill and Pay...\n");
        BackgroundSync.log.writeToFile("A total of " + MainFrame.customersToUpdate.size() + " customers need to be updated to Bill and Pay...\n");
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;            
        }
                
        //Step 2.2 - Compare Invoices
        if(!isCancelled()){
            intermediateJTextArea.append("Step 2.2 - Compare Invoices...\n");
            log.writeToFile("Step 2.2 - Compare Invoices...\n");

            intermediateJTextArea.append("Arranging Bill and Pay Invoices...\n");
            log.writeToFile("Arranging Bill and Pay Invoices...\n");
            orderInvoices(MainFrame.bpInvoices);   
            //setProgress(75);

            intermediateJTextArea.append("Comparing Abila and Bill and Pay Invoices...\n");
            log.writeToFile("Comparing Abila and Bill and Pay Invoices...\n");
            compareInvoices(MainFrame.abilaInvoices, MainFrame.bpInvoices);
            //setProgress(80);

            MainFrame.progressMonitor.append("A total of " + MainFrame.invoicesToAdd.size() + " Invoices need to be added to Bill and Pay...\n");
            log.writeToFile("A total of " + MainFrame.invoicesToAdd.size() + " Invoices need to be added to Bill and Pay...\n");

            intermediateJTextArea.append("Need to update a total of " + MainFrame.invoicesToUpdate.size() + " Invoices in Bill and Pay...\n");
            log.writeToFile("Need to update a total of " + MainFrame.invoicesToUpdate.size() + " Invoices in Bill and Pay...\n");
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        //Step 2.3 - Compare Payments
        if(!isCancelled()){
            intermediateJTextArea.append("Step 2.3 - Compare Payments...\n");
            log.writeToFile("Step 2.3 - Compare Payments...\n");

            intermediateJTextArea.append("Arranging Payments in Order...\n");
            log.writeToFile("Arranging Payments in Order...\n");
            orderPayments(MainFrame.bpPayments);

            /** This code is not needed any more because the BP method payment.info was modified to include the Invoice Reference Number. Not this number is obtained when getting BP Payments originally.
            intermediateJTextArea.append("Getting Bill and Pay Invoice Reference Numbers...\n");
            log.writeToFile("Getting Bill and Pay Invoice Reference Numbers...\n");
            getBPInvoiceReferenceNumbers(MainFrame.bpPayments);**/

            intermediateJTextArea.append("Comparing Abila and Bill and Pay Payments...\n");
            log.writeToFile("Comparing Abila and Bill and Pay Payments...\n");
            comparePayments(MainFrame.abilaPayments, MainFrame.bpPayments);
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }

        total = MainFrame.customersToAdd.size() + MainFrame.customersToUpdate.size() + MainFrame.invoicesToAdd.size() + MainFrame.invoicesToUpdate.size() + MainFrame.paymentsToAddToBillandPay.size() + MainFrame.paymentsToAddToAbila.size() + MainFrame.returnedPayments.size();
        //total = 100;
        value = 0;
        setProgress(0);

        //Step 3 - Perform Sync        
        //Step 3.1 - Sync Customers    
        //Step 3.1.a - Add Customers to BP
        if(!isCancelled()){
            intermediateJTextArea.append("Step 3 - Perform Sync...\n");
            log.writeToFile("Step 3 - Perform Sync...\n");
            intermediateJTextArea.append("Step 3.1 - Sync Customers...\n");
            log.writeToFile("Step 3.1 - Sync Customers...\n");                
            intermediateJTextArea.append("Step 3.1.a - Add Customers to BP...\n");
            log.writeToFile("Step 3.1.a - Add Customers to BP...\n");                

            if(MainFrame.customersToAdd.size() > 0){
                forCount = 1;
                for(Customer c : MainFrame.customersToAdd){
                    if(!isCancelled()){
                       try {
                            pca.performPostCallAddCustomer(buildAddCustomerXML(c));
                        } catch (IOException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        } catch (SAXException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        } catch (ParserConfigurationException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        }

                        MainFrame.progressBarLabels.setText("Adding Customer "+forCount+" of "+MainFrame.customersToAdd.size()); 
                        value = value + 1;
                        forCount = forCount + 1;
                        double percentages = (value/total) * 100;
                        int percentage = (int) percentages;
                        setProgress(percentage);
                    }else{
                        intermediateJTextArea.append("Sync process cancelled...\n");
                        log.writeToFile("Sync process cancelled...\n");
                        log.close();
                        return 100;
                    }
                }
            }else{
                intermediateJTextArea.append("No customers to add to Bill and Pay...\n");
                log.writeToFile("No customers to add to Bill and Pay...\n");
                //log.close();
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }    
             
        
         
        
        //Step 3.1.b - Update Customers in BP
        if(!isCancelled()){
            intermediateJTextArea.append("Step 3.1.b - Update Customers in BP...\n");
            log.writeToFile("Step 3.1.b - Update Customers in BP...\n");                
            forCount = 1;
            for(Customer c : MainFrame.customersToUpdate){

                if(!isCancelled()){
                    try {
                        pcuc.performPostCallUpdateCustomer(builUpdateCustomerXML(c));
                    } catch (IOException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (SAXException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (ParserConfigurationException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    }

                    MainFrame.progressBarLabels.setText("Updating Customer "+forCount+" of "+MainFrame.customersToUpdate.size()); 
                    value = value + 1;
                    forCount = forCount + 1;
                    double percentages = (value/total) * 100;
                    int percentage = (int) percentages;
                    setProgress(percentage);
                }
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 3.2 - Sync Invoices
      
        //Step 3.2.a - Add Invoices to BP
        if(!isCancelled()){
           intermediateJTextArea.append("Step 3.2 - Sync Invoices...\n");
           log.writeToFile("Step 3.2 - Sync Invoices...\n"); 
           intermediateJTextArea.append("Step 3.2.a - Add Invoices to BP...\n");
           log.writeToFile("Step 3.2.a - Add Invoices to BP...\n");             
                
            forCount = 1;
            for(Invoice inv : MainFrame.invoicesToAdd){
                //if(MainFrame.performeCancel){
                    try {
                        pcai.performPostCallAddInvoice(buildAddInvoiceXML(inv));
                    } catch (IOException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (SAXException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (ParserConfigurationException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    }
                    MainFrame.progressBarLabels.setText("Adding Invoice "+forCount+" of "+MainFrame.invoicesToAdd.size());
                    forCount = forCount + 1;
                    value = value + 1;
                    double percentages = (value/total) * 100;
                    int percentage = (int) percentages;
                    setProgress(percentage);           
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        //Step 3.2.a.1 - Send Invoice Emails

        if(sendEmails && prevSync){
            if(!isCancelled()){
                intermediateJTextArea.append("Step 3.2.a.1 - Send Invoice Emails...\n");
                log.writeToFile("Step 3.2.a.1 - Send Invoice Emails...\n");
                for(Invoice inv : MainFrame.invoicesToAdd){
                    if(!isCancelled()){
                        try {
                            pcse.performPostCallSendEmail(buildSendInvoiceEmail(inv));
                        } catch (IOException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        } catch (SAXException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        } catch (ParserConfigurationException ex) {
                            Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                            intermediateJTextArea.append(ex.toString() + "\n");
                            log.writeToFile(ex.toString() + "\n");
                            this.cancel(true);
                        }
                    }
                }
            } else {
                intermediateJTextArea.append("Sync process cancelled...\n");
                log.writeToFile("Sync process cancelled...\n");
                log.close();
                return 100;
                }
        }
        
        //Step 3.2.b - Update Invoices
        if(!isCancelled()){
            intermediateJTextArea.append("Step 3.2.b - Update Invoices...\n");
            log.writeToFile("Step 3.2.b - Update Invoices...\n");                        
      
            forCount = 1;
            for(Invoice inv : MainFrame.invoicesToUpdate){
                if(!isCancelled()){
                    try {
                        pcui.performPostCallUpdateInvoice(buildUpdateInvoiceXML(inv));
                    } catch (IOException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (SAXException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (ParserConfigurationException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    }
                    MainFrame.progressBarLabels.setText("Update Invoice: "+forCount+" of "+MainFrame.invoicesToUpdate.size()+" in Bill and Pay");
                    forCount = forCount + 1;
                    value = value + 1;
                    double percentages = (value/total) * 100;
                    int percentage = (int) percentages;
                    setProgress(percentage);
                    }
                }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 3.3 - Sync Payments
        if(!isCancelled()){
            intermediateJTextArea.append("Step 3.3 - Sync Payments...\n");
            log.writeToFile("Step 3.3 - Sync Payments...\n");

            //Step 3.3.a - Add Payments to Abila
            intermediateJTextArea.append("Step 3.3.a - Add Payments to Abila...\n");
            log.writeToFile("Step 3.3.a - Add Payments to Abila...\n");

            if(MainFrame.paymentsToAddToAbila.size() > 0){
                insertAbilaSession(buildAbilaInsertSession("ARC", "BPSYNC Receipts"));

                insertAbilaDocsandLines(MainFrame.paymentsToAddToAbila);
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 3.3.b - Add Payments to BP
        if(!isCancelled()){
            intermediateJTextArea.append("Step 3.3.b - Add Payments to BP...\n");
            log.writeToFile("Step 3.3.b - Add Payments to BP...\n");
        
            forCount = 1;
            for(Payment pmt : MainFrame.paymentsToAddToBillandPay){
                if(!isCancelled()){
                    try {
                        pcap.performPostCallAddPayment(buildAddPaymentToBPXML(pmt));
                        BackgroundSync.log.writeToFile("Adding Payment ID: " + pmt.getId() + "...\n");
                    } catch (IOException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (SAXException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    } catch (ParserConfigurationException ex) {
                        Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                        intermediateJTextArea.append(ex.toString() + "\n");
                        log.writeToFile(ex.toString() + "\n");
                        this.cancel(true);
                    }
                    MainFrame.progressBarLabels.setText("Adding Payment "+forCount+" of "+MainFrame.paymentsToAddToBillandPay.size()+" to Bill and Pay");
                    forCount = forCount + 1;
                    value = value + 1;
                    double percentages = (value/total) * 100;
                    int percentage = (int) percentages;
                    setProgress(percentage);
                }
            }
        } else {
            intermediateJTextArea.append("Sync process cancelled...\n");
            log.writeToFile("Sync process cancelled...\n");
            log.close();
            return 100;
        }
        
        //Step 3.3.c - Add Returned Payments to Abila
        intermediateJTextArea.append("Step 3.3.c - Add Returned Payments to Abila\n");
        log.writeToFile("Step 3.3.c - Add Returned Payments to Abila\n");
        
        String lastUpdated = "";
        forCount = 1;
        for(Payment p : MainFrame.returnedPayments){
            if(!isCancelled()){
                MainFrame.progressBarLabels.setText("Adding Returned Payment "+forCount+" of "+MainFrame.returnedPayments.size()); 
                value = value + 1;
                forCount = forCount + 1;
                double percentages = (value/total) * 100;
                int percentage = (int) percentages;
                setProgress(percentage);
                
                insertReturnedPayments(p.getCustomerNo(), p.getReferenceNo(), p.getPmtAmount());
                if(lastUpdated.compareTo(p.getBpLastUpdate()) < 0){
                    lastUpdated = p.getBpLastUpdate();
                }
            }else{
                intermediateJTextArea.append("Sync process cancelled...\n");
                log.writeToFile("Sync process cancelled...\n");
                log.close();
                return 100;
            }
        }
        
        if(MainFrame.returnedPayments.size() > 0){
            setLastBPPmtRetrieveDate(lastUpdated);
        }
        
         
        
        setLastSyncDate();        
        MainFrame.progressBarLabels.setText("Sync Completed!");
        intermediateJTextArea.append("Sync Completed!\n");
        log.writeToFile("Sync Completed!\n");
        log.close();
        
        return 0;
    }
    
    protected void done(){
            try {

                int finish = get();

            } catch (Exception e) {
                intermediateJTextArea.append(e.toString() + "\n");
                log.writeToFile(e.toString() + "\n");
                this.cancel(true);
            }
        syncButton.setEnabled(true);
        cancelButton.setEnabled(false);
    }
    
    private void setLastSyncDate(){
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(dateFormat.format(date));
        
        try {
            File file = new File("appproperties.properties");                         
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            properties.setProperty("LastUpdate",dateFormat.format(date));//fileInput.
            fileInput.close();
            File f = new File("appproperties.properties");
            OutputStream out = new FileOutputStream( f );
            properties.store(out, "These is the information required to establish connection to the Abila Database Server and to Bill and Pay.");
            System.out.println(date.toString());
            
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
    
    private void setLastBPPmtRetrieveDate(String lastUpdate){
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        try {
            date = dateFormat.parse(lastUpdate);
        } catch (ParseException ex) {
            Logger.getLogger(BackgroundSync.class.getName()).log(Level.SEVERE, null, ex);
        }
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, 1);
        
        String lastUpdatePlusOneSecond = dateFormat.format(calendar.getTime());
        
        
        
        
        //System.out.println(dateFormat.format(date));
        
        try {
            File file = new File("appproperties.properties");                         
            FileInputStream fileInput = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fileInput);
            properties.setProperty("Last BP Payments Retrieve Date",lastUpdatePlusOneSecond);//fileInput.
            fileInput.close();
            File f = new File("appproperties.properties");
            OutputStream out = new FileOutputStream( f );
            properties.store(out, "These is the information required to establish connection to the Abila Database Server and to Bill and Pay.");
            //System.out.println(date.toString());
            
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
            intermediateJTextArea.append(ex.toString() + "\n");
            log.writeToFile(ex.toString() + "\n");
            this.cancel(true);
        } catch (IOException ex) {
            ex.printStackTrace();
            intermediateJTextArea.append(ex.toString() + "\n");
            log.writeToFile(ex.toString() + "\n");
            this.cancel(true);
        }

    } 
    
    private void getSQLProperties(){
        
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("elitebco100910");

        try {
            File file = new File("appproperties.properties");
            FileInputStream fileInput = new FileInputStream(file);
            //Properties properties = new Properties();
            Properties properties = new EncryptableProperties(encryptor);
            properties.load(fileInput);
            fileInput.close();
            //USER_NAME = encryptor.decrypt(properties.getProperty("UserName"));
            USER_NAME = properties.getProperty("UserName");
            PASSWORD = properties.getProperty("Password");
           // System.out.println(USER_NAME+" "+PASSWORD);
            if(PASSWORD.isEmpty() || USER_NAME.isEmpty()){
                PASSWORD = "BlackBird";
                USER_NAME = "NpsAdmin";
            }
            //properties.getProperty("Password");
            DATABASE_NAME = properties.getProperty("DatabaseName");
            SERVER_NAME = properties.getProperty("SQLServer");
            billerID = properties.getProperty("BillerID");
            billerPassword = properties.getProperty("BillerPassword");
            lastUpdate = properties.getProperty("LastUpdate");
            CheckBoxClass = properties.getProperty("ClassCheckBox");;
            CheckBoxType = properties.getProperty("TypeCheckBox");;
            sStatus = properties.getProperty("Status");;
            sClass = properties.getProperty("Class");;
            sType = properties.getProperty("Type");;
            
            
            DATABASE_URL = "jdbc:jtds:sqlserver://" + properties.getProperty("SQLServer") + ";instance=" + properties.getProperty("Instance") + ";databaseName=" + properties.getProperty("DatabaseName");
             //properties.getProperty("UserName");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
        } catch (IOException e) {
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
        }
    }

    public void getAbilaInvoices(){
        //updateProgress("Getting Invoices from Abila...");
        
        getSQLProperties();
        
        
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
         
            Statement stmt = connection.createStatement();
            ResultSet abilaInvoicesResultSet;
            
            String testSQL = "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" + 
                                                            "\n" +
                                                            "SELECT   tblAROpenDoc.dDocID as id\n" +
                                                            "		,tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                            "		,tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                            "		,tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                            "		,tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                            "		,tblARCustomer.sBillingCity as billingCity\n" +
                                                            "		,tblARCustomer.sBillingState as billingState\n" +
                                                            "		,tblARCustomer.sBillingZip as billingZip\n" +
                                                            "		,1 as lineItemQty\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                            "		,tblDLDocument.sDescription as lineItemDescription\n" +
                                                            "\n" +
                                                            "	FROM [dbo].[tblAROpenDoc]\n" +
                                                            "	inner join dbo.tblARCustomer\n" +
                                                            "		on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "	inner join [dbo].tblDLDocument\n" +
                                                            "		on tblAROpenDoc.sDocNum = tblDLDocument.sDocNum and tblAROpenDoc.sPlayerNumIDf = tblDLDocument.sPlayerNumIDf\n" +
                                                            "	where tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP' and tblDLDocument.dtmPosted > @LastUpdate " + whereClause + "\n" +
                                                            "	order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf";
            System.out.println(testSQL);
            
            if(MainFrame.lastUpdate.isEmpty()){prevSync = false;}
            //Previously Synced?
            if(prevSync){
                //Previously synced, only bring new and updated invoices
                abilaInvoicesResultSet = stmt.executeQuery( "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" + 
                                                            "\n" +
                                                            "SELECT   tblAROpenDoc.dDocID as id\n" +
                                                            "		,tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                            "		,tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                            "		,tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                            "		,tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                            "		,tblARCustomer.sBillingCity as billingCity\n" +
                                                            "		,tblARCustomer.sBillingState as billingState\n" +
                                                            "		,tblARCustomer.sBillingZip as billingZip\n" +
                                                            "		,1 as lineItemQty\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                            "		,tblDLDocument.sDescription as lineItemDescription\n" +
                                                            "\n" +
                                                            "	FROM [dbo].[tblAROpenDoc]\n" +
                                                            "	inner join dbo.tblARCustomer\n" +
                                                            "		on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "	inner join [dbo].tblDLDocument\n" +
                                                            "		on tblAROpenDoc.sDocNum = tblDLDocument.sDocNum and tblAROpenDoc.sPlayerNumIDf = tblDLDocument.sPlayerNumIDf\n" +
                                                            "	where tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP' and tblDLDocument.dtmPosted > @LastUpdate " + whereClause + "\n" +
                                                            "	order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf");
                
                
                
                
                //Old query that used where clause to limit certain customer transactions. 
                /**abilaInvoicesResultSet = stmt.executeQuery( "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" + 
                                                            "\n" +
                                                            "SELECT   tblAROpenDoc.dDocID as id\n" +
                                                            "		,tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                            "		,tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                            "		,tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                            "		,tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                            "		,case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                            "		,tblARCustomer.sBillingCity as billingCity\n" +
                                                            "		,tblARCustomer.sBillingState as billingState\n" +
                                                            "		,tblARCustomer.sBillingZip as billingZip\n" +
                                                            "		,1 as lineItemQty\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                            "           ,cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                            "		,tblDLDocument.sDescription as lineItemDescription\n" +
                                                            "\n" +
                                                            "	FROM [dbo].[tblAROpenDoc]\n" +
                                                            "	inner join dbo.tblARCustomer\n" +
                                                            "		on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "	inner join [dbo].tblDLDocument\n" +
                                                            "		on tblAROpenDoc.dDocID = tblDLDocument.ctrDocID\n" +
                                                            "	where tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP' and tblDLDocument.dtmPosted > @LastUpdate " + appendedWhereClause + "\n" +
                                                            "	order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf"); **/
            }else{
                //Not previously synced, so decide whether to sync all or only records related to transactions with balances
                if(syncTransWithBal){
                //Get Invoices with Open Balances only
                abilaInvoicesResultSet = stmt.executeQuery("SELECT tblAROpenDoc.dDocID as id\n" +
                                                            ",tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                            ",tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                            ",tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                            ",tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                            ",tblARCustomer.sBillingCity as billingCity\n" +
                                                            ",tblARCustomer.sBillingState as billingState\n" +
                                                            ",tblARCustomer.sBillingZip as billingZip\n" +
                                                            ",1 as lineItemQty\n" +
                                                            ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                            ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                            ",tblDLDocument.sDescription as lineItemDescription\n" +

                                                            "FROM [dbo].[tblAROpenDoc]\n" +
                                                            "inner join dbo.tblARCustomer\n" +
                                                                "on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "inner join [dbo].tblDLDocument\n" +
                                                                "on tblAROpenDoc.dDocID = tblDLDocument.ctrDocID\n" +
                                                            "where tblAROpenDoc.curAmount != 0 and tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP'\n" +                 
                                                            "order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf");
                
                
                
                
                
                
                //Old query that used where clause to limit certain custmer transactions. 
                /**abilaInvoicesResultSet = stmt.executeQuery("SELECT tblAROpenDoc.dDocID as id\n" +
                                                            ",tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                            ",tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                            ",tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                            ",tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                            ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                            ",tblARCustomer.sBillingCity as billingCity\n" +
                                                            ",tblARCustomer.sBillingState as billingState\n" +
                                                            ",tblARCustomer.sBillingZip as billingZip\n" +
                                                            ",1 as lineItemQty\n" +
                                                            ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                            ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                            ",tblDLDocument.sDescription as lineItemDescription\n" +

                                                            "FROM [dbo].[tblAROpenDoc]\n" +
                                                            "inner join dbo.tblARCustomer\n" +
                                                                "on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "inner join [dbo].tblDLDocument\n" +
                                                                "on tblAROpenDoc.dDocID = tblDLDocument.ctrDocID\n" +
                                                            "where tblAROpenDoc.curAmount != 0 and tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP' " + appendedWhereClause + "\n" +                 
                                                            "order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf"); **/
                }else{
                    //Get All Invoices Query
                    abilaInvoicesResultSet = stmt.executeQuery("SELECT tblAROpenDoc.dDocID as id\n" +
                                                                ",tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                                ",tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                                ",tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                                ",tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                                ",tblARCustomer.sBillingCity as billingCity\n" +
                                                                ",tblARCustomer.sBillingState as billingState\n" +
                                                                ",tblARCustomer.sBillingZip as billingZip\n" +
                                                                ",1 as lineItemQty\n" +
                                                                ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                                ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                                ",tblDLDocument.sDescription as lineItemDescription\n" +

                                                            "FROM [dbo].[tblAROpenDoc]\n" +
                                                            "inner join dbo.tblARCustomer\n" +
                                                                "on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "inner join [dbo].tblDLDocument\n" +
                                                                "on tblAROpenDoc.dDocID = tblDLDocument.ctrDocID\n" +
                                                            "where tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP'\n" +                   
                                                            "order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf");
                    
                    
                    
                    //Old query that used where clause to limit certain custmer transactions. 
                    /**abilaInvoicesResultSet = stmt.executeQuery("SELECT tblAROpenDoc.dDocID as id\n" +
                                                                ",tblAROpenDoc.sDocNum as referenceNumber\n" +
                                                                ",tblAROpenDoc.dtmDocDate as invoiceDate\n" +
                                                                ",tblAROpenDoc.dtmDueDate as dueDate\n" +
                                                                ",tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) =0 then tblARCustomer.sBillingAddress else SUBSTRING(tblARCustomer.sBillingAddress,0,charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress1	  \n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) =0 then SUBSTRING(tblARCustomer.sBillingAddress,charindex(char(13),tblARCustomer.sBillingAddress) + 2,len(tblARcustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress)) end as billingAddress2\n" +
                                                                ",case when charindex(char(13),tblARCustomer.sBillingAddress) !=0 and charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) !=0 then SUBSTRING(tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress) + 1) + 2, len(tblARCustomer.sBillingAddress) - charindex(char(13),tblARCustomer.sBillingAddress, charindex(char(13),tblARCustomer.sBillingAddress))) end as billingAddress3\n" +
                                                                ",tblARCustomer.sBillingCity as billingCity\n" +
                                                                ",tblARCustomer.sBillingState as billingState\n" +
                                                                ",tblARCustomer.sBillingZip as billingZip\n" +
                                                                ",1 as lineItemQty\n" +
                                                                ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemPrice\n" +
                                                                ",cast(round((select sum(tblDLTrans.curAmount) FROM tblDLTrans where tblDLTrans.dMatchDocIDf = tblAROpenDoc.dDocID and tblDLTrans.sInvSrcCurrencyIDf != '' and  tblDLTrans.sOrigTransSourceIDf in ('ARS','ARB','ARV')),2) as numeric(36,2)) as lineItemTotal\n" +
                                                                ",tblDLDocument.sDescription as lineItemDescription\n" +

                                                            "FROM [dbo].[tblAROpenDoc]\n" +
                                                            "inner join dbo.tblARCustomer\n" +
                                                                "on tblARCustomer.sCustomerID = tblAROpenDoc.sPlayerNumIDf\n" +
                                                            "inner join [dbo].tblDLDocument\n" +
                                                                "on tblAROpenDoc.dDocID = tblDLDocument.ctrDocID\n" +
                                                            "where tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP' " + appendedWhereClause + "\n" +                   
                                                            "order by tblAROpenDoc.sDocNum, tblAROpenDoc.sPlayerNumIDf");**/
                }
            }

            while(abilaInvoicesResultSet.next()){
                String id = abilaInvoicesResultSet.getString("id");
                String rn = abilaInvoicesResultSet.getString("referenceNumber");
                String invdt = abilaInvoicesResultSet.getString("invoiceDate");
                String dd = abilaInvoicesResultSet.getString("dueDate");
                String cid = abilaInvoicesResultSet.getString("CustomerID");
                String bal1 = abilaInvoicesResultSet.getString("billingAddress1");
                String bal2 = abilaInvoicesResultSet.getString("billingAddress2");
                String bal3 = abilaInvoicesResultSet.getString("billingAddress3");
                String bc = abilaInvoicesResultSet.getString("billingCity");
                String bs = abilaInvoicesResultSet.getString("billingState");
                String bz = abilaInvoicesResultSet.getString("billingZip");
                String qty = abilaInvoicesResultSet.getString("lineItemQty");
                String ip = abilaInvoicesResultSet.getString("lineItemPrice");
                String it = abilaInvoicesResultSet.getString("lineItemTotal");
                String idesc = abilaInvoicesResultSet.getString("lineItemDescription");                
                
                Invoice i = new Invoice(invdt, dd, id, rn, cid, bal1, bal2, bal3, bc, bs, bz, qty, ip, it, idesc);
                
                MainFrame.abilaInvoices.add(i);   
                
                
            }
            
        }catch (Exception e){
            
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
        }
        
        //int ctr = 0;
        
    }
    
    private static void orderInvoices(ArrayList<Invoice> invoices) {

        Collections.sort(invoices, new Comparator() {

            public int compare(Object o1, Object o2) {

                String rNo1 = ((Invoice) o1).getReferenceNumber();
                String rNo2 = ((Invoice) o2).getReferenceNumber();
                int sComp = rNo1.compareTo(rNo2);

                if (sComp != 0) {
                   return sComp;
                } else {
                   String cusID1 = ((Invoice) o1).getCustomerID();
                   String cusID2 = ((Invoice) o2).getCustomerID();
                   return cusID1.compareTo(cusID2);
                }
            }
        });
    }
    
    private void compareInvoices(ArrayList<Invoice> a, ArrayList<Invoice> b){
        boolean keepLooking = true;
        //boolean equal = false;
        int bpInvCtr = 0;
        int abilaInvCtr = 0;
        
        //need to address the case when bp does not have any customers. Then the entire list has to sync. ** I beleive it is addressed in the last if statement
        for(Invoice ai : a){
            
            
            keepLooking = true;
            
            //Get Abila Invoice Info
            String refA = ai.getReferenceNumber();
            String cusIDA = ai.getCustomerID();
            String totalA = ai.getLineItemTotal();
            
            

            while(keepLooking && bpInvCtr < b.size()){
                //Get Bil and Pay Invoice Info
                String refB = b.get(bpInvCtr).getReferenceNumber();
                String cusIDB = b.get(bpInvCtr).getCustomerID();
                String totalB = b.get(bpInvCtr).getLineItemTotal();
                
                //System.out.println("Comparing Abila Invoice No.: " + refA + " - Abila Client ID: " + cusIDA + " - Abila Amout $" + totalA + " to BP Invoice No.: " + refB + " - BP Client ID: " + cusIDB + " - BP Amout $" + totalB + "\n");
                
                
                
                //String progressTest = "Comparing Abila Invoice " + refA + " to BP Invoice " + refB;
                
                //MainFrame.progressMonitor.append(progressTest + "\n");
                
                if(refA.toUpperCase().equals(refB.toUpperCase())){
                    /**if(refB.equals("46227")){
                    System.out.println("Found it!");
                    }**/
                    if(cusIDA.toUpperCase().equals(cusIDB.toUpperCase())){
                        keepLooking = false;
                        bpInvCtr++;
                        if(totalA.equals(totalB)){
                            //Invoice found and doesn't need to be added
                        }else{
                            //Invoice was found but the total changed
                            MainFrame.invoicesToUpdate.add(ai);
                            //System.out.println("Invoice needs to be updated.\n");
                            //bpInvCtr++;
                        }
                    }else{
                        //Another customer has the same invoice number and needs to be added
                        keepLooking = false;
                        MainFrame.invoicesToAdd.add(ai);
                        //System.out.println("Invoice needs to be added bcse another customer has same invoice number.\n");
                        //bpInvCtr++;
                    }                    
                }else if(refA.toUpperCase().compareTo(refB.toUpperCase()) < 0){
                    //BP invoice is greater; add current Abila invoice to BP and start looking for next Abila invoice.
                    //System.out.println("Invoice needs to be added.\n");
                    MainFrame.invoicesToAdd.add(ai);
                    keepLooking = false;
                    //bpInvCtr++;
                }else{
                    bpInvCtr++;
                    //System.out.println("BP Invoice is smaller and needs to be skipped and start search with new Abila Invoice No.\n");
                }
                
            }
                
            abilaInvCtr++;
                
            if(keepLooking && bpInvCtr >= b.size()){
                MainFrame.invoicesToAdd.add(ai);
            }
        }
        
                
        
        /**System.out.println("Invoices to add lists:\n");
        for(Invoice inv : MainFrame.invoicesToAdd){
            System.out.println(inv.toString());
        }**/
    }
    
    private String buildAddInvoiceXML(Invoice inv){
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <invoiceadd> ";
        
        
        xml += "<billingaddress> <address1>" + inv.getBillingAddress1() + "</address1> <address2>" + inv.getBillingAddress2() + "</address2> <address3>" +  inv.getBillingAddress3() + "</address3> <city>" + 
                inv.getBillingCity() + "</city> <state>" + inv.getBillingState() + "</state> <zip>" + inv.getBillingZip() + "</zip> </billingaddress> <number>" + inv.getReferenceNumber() + "</number> <id>" + 
                inv.getId() + "</id> <customer> <id>" + inv.getCustomerID() + "</id> </customer> <createddate>" + inv.getInvoiceDate() + "</createddate> <duedate>" + inv.getDueDate() + "</duedate> <lineitem> <id>DEFAULT</id> <quantity>" + inv.getLineItemQty() + 
                "</quantity> <rate>" + inv.getLineItemPrice() + "</rate> <amount>" + inv.getLineItemTotal() + "</amount> <description>" + inv.getLineItemDescription() + "</description> </lineitem>";        
        
        xml += "</invoiceadd> </biller> </request>";
        
        //System.out.println(xml);
        
        return xml;
    }
    
    private String buildUpdateInvoiceXML(Invoice inv){
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <invoiceupdate> ";
        
        
        xml += "<id>" + inv.getId() + "</id> <lineitem> <id>DEFAULT</id> <rate>" + inv.getLineItemPrice() + "</rate> </lineitem>";        
        
        xml += " </invoiceupdate> </biller> </request>";       
        
        System.out.println(xml);
        
        return xml;
    }
    
    private void getAbilaCustomers(){        
        intermediateJTextArea.append("Getting Customers from Abila...\n");
        log.writeToFile("Getting Customers from Abila...\n");
        
        getSQLProperties();
        
        /**This is to construct the where clause from all the different filters for Type, Class, and status. This makes the system too slow so will eliminate for now.
        if(!sStatus.replaceAll("\\[","").replaceAll("\\]","").matches("")){
            whereClause = "where sStatus IN"+sStatus.replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
        }
        **/
    
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
         
            Statement stmt = connection.createStatement();
            

            /**This is to construct the where clause from all the different filters for Type, Class, and status. This makes the system too slow so will eliminate for now.
            if(!Boolean.valueOf(CheckBoxClass)){
                if(whereClause.isEmpty()){
                    whereClause = "where sClass IN" + sClass.replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
                } else {
                    if(!sClass.replaceAll("\\[","").replaceAll("\\]","").matches("")){
                        whereClause = whereClause + " and sClass IN" + sClass.replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
                    }
                }
            }
            
            if(!Boolean.valueOf(CheckBoxType)){
                if(whereClause.isEmpty()){
                    whereClause = "where sType IN" + sType.replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
                } else {
                    if(!sType.replaceAll("\\[","").replaceAll("\\]","").matches("")){
                        whereClause = whereClause + " and sType IN" + sType.replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
                    }                    
                }
                
            }
            
            //Substitute <blank>
            whereClause = whereClause.replace("<blank>", "");
            
            //Assign value to appendedWhereClause for queries that have other where clauses.
            if(!whereClause.equals("")){
                appendedWhereClause = "and " + whereClause.substring(6);
            }
            **/
      
      
            if(!customerToSync.isEmpty()){
                 if(whereClause.isEmpty()){
                     whereClause = "and sCustomerID = '" + customerToSync + "'";
                 } else{
                     whereClause = whereClause + "and sCustomerID = '" + customerToSync + "'";
                 }
                
            }
            

            
//            else{
//                ResultSet classResultSet;
//                classResultSet = stmt.executeQuery("SELECT DISTINCT [sClass] FROM [dbo].[tblARCustomer] ORDER BY sClass ASC");
//
//                List<String> classList = new ArrayList<String>();
//                while(classResultSet.next()){
//                    if(classResultSet.getString("sClass").matches("")){
//                        classList.add(" ");
//                    }else{
//                        classList.add(classResultSet.getString("sClass").replaceAll("\\s+",""));
//                    }
//                }
//                whereClause = whereClause + " and sClass IN"+classList.toString().replaceAll("\\[", "\\('").replaceAll("\\]", "\\')").replaceAll(",","','");
//            }

            System.out.println(whereClause);
                System.out.println("DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "select A.* from (SELECT	 [sCustomerID] As id\n" +
                                                            "		,[sName] As companyname\n" +
                                                            "		,[sBillingContactFirstName] as firstname\n" +
                                                            "		,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                            "		,[sBillingContactLastName] as lastname\n" +
                                                            "		,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                            "		,[sBillingContactEMail] as email\n" +
                                                            "FROM [dbo].[tblARCustomer]\n" +
                                                            "-- " + whereClause + " \n" +//this makes the query very slow. In addition, changing filters complicates the algorithm tremendously. We will not use any filters. LBB@2019.08.01: I added it back so that we can limit the query to a particular customer to resolve the issue of applied prepayments not showing in Bill and Pay. 
                                                            " ) as A \n" +  
                                                            "\n" +
                                                            "inner join\n" +
                                                            "\n" +
                                                            "(select distinct *  from (select sPlayerNumIDf as CustomerID\n" +
                                                            "from dbo.tblDLDocument\n" +
                                                            "where dtmPosted > @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Edited%' \n" +
                                                            "	AND dtmDateStamp >= @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Added%'\n" +
                                                            "	AND dtmDateStamp >= @LastUpdate) as B) as C\n" +
                                                            "\n" +
                                                            "on C.CustomerID = A.id\n" +
                                                            "\n" +
                                                            "order by A.id");
             
            /*String testSQL = "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "select A.* from (SELECT	 [sCustomerID] As id\n" +
                                                            "		,[sName] As companyname\n" +
                                                            "		,[sBillingContactFirstName] as firstname\n" +
                                                            "		,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                            "		,[sBillingContactLastName] as lastname\n" +
                                                            "		,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                            "		,[sBillingContactEMail] as email\n" +
                                                            "FROM [dbo].[tblARCustomer]\n" +
                                                            "where sClass = 'BPSync') as A\n" +                        
                                                            "\n" +
                                                            "inner join\n" +
                                                            "\n" +
                                                            "(select distinct *  from (select sPlayerNumIDf as CustomerID\n" +
                                                            "from dbo.tblDLDocument\n" +
                                                            "where dtmPosted > @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Edited%' \n" +
                                                            "	AND dtmDateStamp >= @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Added%'\n" +
                                                            "	AND dtmDateStamp >= @LastUpdate) as B) as C\n" +
                                                            "\n" +
                                                            "on C.CustomerID = A.id\n" +
                                                            "\n" +
                                                            "order by A.id";
            System.out.println(testSQL);*/
            
            ResultSet abilaCustomersResultSet;
            if(MainFrame.lastUpdate.isEmpty()){prevSync=false;}
            //Previously Synced?
            if(prevSync){
                //Previously synced, only bring new, updated, and new transactions related customers
                abilaCustomersResultSet = stmt.executeQuery("DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "select A.* from (SELECT	 [sCustomerID] As id\n" +
                                                            "		,[sName] As companyname\n" +
                                                            "		,[sBillingContactFirstName] as firstname\n" +
                                                            "		,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                            "		,[sBillingContactLastName] as lastname\n" +
                                                            "		,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                            "		,[sBillingContactEMail] as email\n" +
                                                            "FROM [dbo].[tblARCustomer]\n" +
                                                            "-- " + whereClause + " \n" +//this makes the query very slow. In addition, changing filters complicates the algorithm tremendously. We will not use any filters. LBB@2019.08.01: I added it back so that we can limit the query to a particular customer to resolve the issue of applied prepayments not showing in Bill and Pay. 
                                                            " ) as A \n" +  
                                                            "\n" +
                                                            "inner join\n" +
                                                            "\n" +
                                                            "(select distinct *  from (select sPlayerNumIDf as CustomerID\n" +
                                                            "from dbo.tblDLDocument\n" +
                                                            "where dtmPosted > @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Edited%' \n" +
                                                            "	AND dtmDateStamp >= @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Added%'\n" +
                                                            "	AND dtmDateStamp >= @LastUpdate) as B) as C\n" +
                                                            "\n" +
                                                            "on C.CustomerID = A.id\n" +
                                                            "\n" +
                                                            "order by A.id");
                
                //Old query that used where clause to limit certain customers. 
                /**abilaCustomersResultSet = stmt.executeQuery("DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "select A.* from (SELECT	 [sCustomerID] As id\n" +
                                                            "		,[sName] As companyname\n" +
                                                            "		,[sBillingContactFirstName] as firstname\n" +
                                                            "		,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                            "		,[sBillingContactLastName] as lastname\n" +
                                                            "		,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                            "		,[sBillingContactEMail] as email\n" +
                                                            "FROM [dbo].[tblARCustomer]\n" +
                                                            whereClause + " ) as A \n" +                       
                                                            "\n" +
                                                            "inner join\n" +
                                                            "\n" +
                                                            "(select distinct *  from (select sPlayerNumIDf as CustomerID\n" +
                                                            "from dbo.tblDLDocument\n" +
                                                            "where dtmPosted > @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Edited%' \n" +
                                                            "	AND dtmDateStamp >= @LastUpdate\n" +
                                                            "\n" +
                                                            "union\n" +
                                                            "\n" +
                                                            "SELECT RTRIM(LTRIM(SUBSTRING(sMessage,CHARINDEX(':',[sMessage])+1,CHARINDEX('was',[sMessage])-CHARINDEX(':',[sMessage])-1))) as CustomerID\n" +
                                                            "FROM dbo.tblOrgActivityLog\n" +
                                                            "WHERE [sMessageType] like 'AR_NPS-GUI_CBOMaintainCustomers_Added%'\n" +
                                                            "	AND dtmDateStamp >= @LastUpdate) as B) as C\n" +
                                                            "\n" +
                                                            "on C.CustomerID = A.id\n" +
                                                            "\n" +
                                                            "order by A.id");**/

            }else{
                //Not previously synced, so decide whether to sync all or only records related to transactions with balances
                if(syncTransWithBal){
                    //Get Customers with Open Balances only
                    abilaCustomersResultSet = stmt.executeQuery("SELECT	 [sCustomerID] As id\n" +
                                                                "        ,[sName] As companyname\n" +
                                                                "        ,[sBillingContactFirstName] as firstname\n" +
                                                                "        ,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                                "        ,[sBillingContactLastName] as lastname\n" +
                                                                "        ,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                                "        ,[sBillingContactEMail] as email\n" +
                                                                "FROM [dbo].[tblARCustomer]\n" +
                                                                "inner join\n" +
                                                                "(SELECT distinct tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                                "FROM [dbo].[tblAROpenDoc]\n" +
                                                                "where [dbo].[tblAROpenDoc].curAmount != 0) as B\n" +
                                                                "on B.CustomerID = [dbo].[tblARCustomer].sCustomerID\n");
                    
                    //Old query that used where clause to limit certain customers. 
                    /**abilaCustomersResultSet = stmt.executeQuery("SELECT	 [sCustomerID] As id\n" +
                                                                "        ,[sName] As companyname\n" +
                                                                "        ,[sBillingContactFirstName] as firstname\n" +
                                                                "        ,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                                "        ,[sBillingContactLastName] as lastname\n" +
                                                                "        ,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                                "        ,[sBillingContactEMail] as email\n" +
                                                                "FROM [dbo].[tblARCustomer]\n" +
                                                                "inner join\n" +
                                                                "(SELECT distinct tblAROpenDoc.sPlayerNumIDf as CustomerID\n" +
                                                                "FROM [dbo].[tblAROpenDoc]\n" +
                                                                "where [dbo].[tblAROpenDoc].curAmount != 0) as B\n" +
                                                                "on B.CustomerID = [dbo].[tblARCustomer].sCustomerID\n" + whereClause);**/
                }else{
                    //Get all Customers
                    abilaCustomersResultSet = stmt.executeQuery("SELECT	 [sCustomerID] As id\n" +
                                                                "        ,[sName] As companyname\n" +
                                                                "        ,[sBillingContactFirstName] as firstname\n" +
                                                                "        ,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                                "        ,[sBillingContactLastName] as lastname\n" +
                                                                "        ,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                                "        ,[sBillingContactEMail] as email\n" +
                                                                "FROM [dbo].[tblARCustomer]\n");
                    
                    //Old query that used where clause to limit certain customers.
                    /**abilaCustomersResultSet = stmt.executeQuery("SELECT	 [sCustomerID] As id\n" +
                                                                "        ,[sName] As companyname\n" +
                                                                "        ,[sBillingContactFirstName] as firstname\n" +
                                                                "        ,[sBillingContactMiddleInitial] as middleinitial\n" +
                                                                "        ,[sBillingContactLastName] as lastname\n" +
                                                                "        ,case when [sStatus] = 'A' then 1 else 0 end As 'status'\n" +
                                                                "        ,[sBillingContactEMail] as email\n" +
                                                                "FROM [dbo].[tblARCustomer]\n" + whereClause);**/
                }
            }
        
            while(abilaCustomersResultSet.next()){
                String id = abilaCustomersResultSet.getString("id");
                String cn = abilaCustomersResultSet.getString("companyname");
                String fn = abilaCustomersResultSet.getString("firstname");
                String mi = abilaCustomersResultSet.getString("middleinitial");
                String ln = abilaCustomersResultSet.getString("lastname");
                String s = abilaCustomersResultSet.getString("status");
                String e = abilaCustomersResultSet.getString("email");
                
                Customer c = new Customer(id, cn, fn, mi, ln, s, e);
                System.out.println("Adding customer " + c.getId());
                MainFrame.abilaCutomers.add(c);                
            }
            
        }catch (Exception e){
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            //this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
            
        }
        
        intermediateJTextArea.append("Retrieved a total of " + (MainFrame.abilaCutomers.size()) + " Customers from Abila...\n");
        log.writeToFile("Retrieved a total of " + (MainFrame.abilaCutomers.size()) + " Customers from Abila...\n");
    }
    
    private static void orderCustomers(ArrayList<Customer> customers) {

        log.writeToFile("Sorting Abila customers...\n");
        Collections.sort(customers, new Comparator() {
            public int compare(Object o1, Object o2) {

                String rNo1 = ((Customer) o1).getId();
                String rNo2 = ((Customer) o2).getId();
                return rNo1.toUpperCase().compareTo(rNo2.toUpperCase());
            }
        });
    }
    
    private void compareCustomers(ArrayList<Customer> a, ArrayList<Customer> b){
        intermediateJTextArea.append("Comparing BP and Abila Customers...\n");
        log.writeToFile("Comparing BP and Abila Customers...\n");
        boolean keepLooking = true;
        //boolean equal = false;
        int bpCusCtr = 0;
        int abilaCusCtr = 0;        
        

        
        for(Customer ac : a){
            //Flag to control while loop
            keepLooking = true;
            
            //Get Abila Customer Info
            String idA = ac.getId();
            String statusA = ac.getStatus();  
            String emailA = ac.getEmail().toUpperCase();
            String nameA = ac.getFullName();
            String compNameA = ac.getCompanyName();
            
            while(keepLooking && bpCusCtr < b.size()){
                //Get Bill and Pay Customer Info
                String idB = b.get(bpCusCtr).getId();
                String statusB = b.get(bpCusCtr).getStatus(); 
                String emailB = b.get(bpCusCtr).getEmail();
                String nameB = b.get(bpCusCtr).getFullName();
                String compNameB = b.get(bpCusCtr).getCompanyName();
                if(emailB == null){
                    emailB = "";
                }
                
                //For troubleshooting... eliminate
                log.writeToFile("Comparing BP Customer ID: " + idB + " and Abila Customer ID: " + idA + " - ");   
                if(idB.equals("at&t")){
                    System.out.println("Found at&t\n");
                    String test = emailB.toUpperCase();
                }
                
                
                if(idA.toUpperCase().equals(idB.toUpperCase())){
                    keepLooking = false;
                    if(statusA.equals(statusB) && emailA.toUpperCase().equals(emailB.toUpperCase()) && nameA.equals(nameB) && compNameA.equals(compNameB)){
                        //Customer found without change; no further action required
                        
                        //For troubleshooting... eliminate
                        log.writeToFile("Customer found without change; no further action required\n");
                    }else{
                        //Customer was found with changes and needs to be updated
                        
                        //For troubleshooting... eliminate
                        log.writeToFile("Customer was found with changes and needs to be update\n");
                        MainFrame.customersToUpdate.add(ac);
                    }
                    bpCusCtr++;
                }else if(idA.toUpperCase().compareTo(idB.toUpperCase()) < 0){
                    //BP customer is greater; add current Abila customer to BP and start looking for next Abila customer.
                    
                    //For troubleshooting... eliminate
                    log.writeToFile("BP customer is greater; add current Abila customer to BP and start looking for next Abila customer\n");
                    MainFrame.customersToAdd.add(ac);
                    keepLooking = false;
                }else{
                    //Abila customer is greater; increment BP Cutomer and keep looking.
                    
                    //For troubleshooting... eliminate
                    log.writeToFile("Abila customer is greater; increment BP Cutomer and keep looking\n");
                    bpCusCtr++;
                }
            }
                
            if(keepLooking && bpCusCtr >= b.size()){
                //For troubleshooting... eliminate
                log.writeToFile("Abila Customer ID: " + idA + " added because BP Customer array end was reached\n");
                
                //Add all remaining Abila Customers when the end of the BP Customer array is reached.
                MainFrame.customersToAdd.add(ac);
            }
        }
    }
    
    private String buildAddCustomerXML(Customer c){
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <customeradd> ";
        //int active = 1;
        
        //for(Customer c: customersToAdd){
        /**if(c.getStatus() != "A"){
            active =**/
        
            
        xml += "<id>" + c.getId() + "</id> <active>"  + c.getStatus() +  "</active> <companyname>" + c.getCompanyName() + "</companyname> <firstname>" + c.getFirstName() + "</firstname> <middlename>" + c.getMiddleInitial() + "</middlename> <lastname>" + c.getLastName() + "</lastname> <email>" + c.getEmail() + "</email> ";
        
        
        xml += "</customeradd> </biller> </request>";
        
        System.out.println(xml);
        
        return xml;
    }
    
    private String builUpdateCustomerXML(Customer c){
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <customerupdate> ";
        //int active = 1;
        
        //for(Customer c: customersToAdd){
        /**if(c.getStatus() != "A"){
            active = 0;
        }**/
            
        xml += "<id>" + c.getId() + "</id> <active>" + c.getStatus() + "</active> <companyname>" + c.getCompanyName() + "</companyname> <firstname>" + c.getFirstName() + "</firstname> <middlename>" + c.getMiddleInitial() + "</middlename> <lastname>" + c.getLastName() + "</lastname> <email>" + c.getEmail() + "</email> ";
        
        
        xml += "</customerupdate> </biller> </request>";
        
        //System.out.println(xml);
        
        return xml;
    }
    
    public void getAccountingSegments(){
        MainFrame.accountSegments.clear();
        intermediateJTextArea.append("Getting Abila Accounting Segments Information...\n");
        log.writeToFile("Getting Abila Accounting Segments Information...\n");
        getSQLProperties();
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
            Statement stmt = connection.createStatement();          
            ResultSet accountingSegmentsResultSet;
            
            accountingSegmentsResultSet = stmt.executeQuery("SELECT [nSegmentID]\n" +
                                                            "      ,[sTitle]\n" +
                                                            "      ,[nLength]\n" +
                                                            "      ,[sSegType]\n" +
                                                            "  FROM [tblSegmentInfo]");
                
        
            int ctr = 0;
            while(accountingSegmentsResultSet.next()){
                String id = accountingSegmentsResultSet.getString("nSegmentID");
                String sn = accountingSegmentsResultSet.getString("sTitle");
                String sl = accountingSegmentsResultSet.getString("nLength");
                String st = trim(accountingSegmentsResultSet.getString("sSegType"));
                
                if(st.equals("GL")){
                    MainFrame.glSegment = "sCodeIDf_" + id;
                    MainFrame.glSegmentIndex = ctr;                  
                }else if(st.equals("FUND")){
                    MainFrame.fundSegment = "sCodeIDf_" + id;
                    MainFrame.fundSegmentIndex = ctr;
                }
                
                AccountSegment as = new AccountSegment(sn, id, sl, st);
                MainFrame.accountSegments.add(as); 
                ctr++;
            }
            
        }catch (Exception e){
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
        }
        
        intermediateJTextArea.append("Retrieved a total of " + (MainFrame.accountSegments.size()) + " Accounting Segments from Abila...\n");
        log.writeToFile("Retrieved a total of " + (MainFrame.accountSegments.size()) + " Accounting Segments from Abila...\n");
    }
    
    public void getRequiredSegments(){
        MainFrame.requiredGLAssigments.clear();
        getSQLProperties();
        
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
            Statement stmt = connection.createStatement();          
            ResultSet requiredSegmentsResultSet;
            
            requiredSegmentsResultSet = stmt.executeQuery("SELECT  [sCodeIDf]\n" +
                                                            "       ,[nRequiredSegmentID]\n" +
                                                            "FROM [tblAcctCodeDetail_0]\n" +
                                                            "order by sCodeIDf asc, nRequiredSegmentID asc");
                
        
            String priorGLCode = "";
            ArrayList<Integer> reqSegmentsTemp = new ArrayList<Integer>(); 
            
            while(requiredSegmentsResultSet.next()){
         
                String glCode = requiredSegmentsResultSet.getString("sCodeIDf");
                Integer reqSegment = Integer.parseInt(requiredSegmentsResultSet.getString("nRequiredSegmentID"));
            

                if(glCode.equals(priorGLCode) || priorGLCode == ""){
                    reqSegmentsTemp.add(reqSegment);
                    priorGLCode = glCode;
                }else{                    
                    ArrayList<Integer> newList = new ArrayList<>(reqSegmentsTemp);
                    AccountCodeAssignments acs = new AccountCodeAssignments(priorGLCode, newList);
                    MainFrame.requiredGLAssigments.add(acs);
                    reqSegmentsTemp.clear();
                    reqSegmentsTemp.add(reqSegment);
                    priorGLCode = "";
                }                            
            }
            
            //Add last record
            AccountCodeAssignments acs = new AccountCodeAssignments(priorGLCode, reqSegmentsTemp);
            MainFrame.requiredGLAssigments.add(acs);            
            
        }catch (Exception e){
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
        }
    }
    
    public void getAbilaPayments(){
        
        intermediateJTextArea.append("Getting Payments from Abila...\n");
        log.writeToFile("Getting Payments from Abila...\n");
        
        //getSQLProperties();  
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
         
            Statement stmt = connection.createStatement();
            ResultSet abilaPaymentsResultSet;
            
            String testSQL = "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "Select * from(SELECT dbo.tbldltrans.[dDocID] as pmtID\n" +
                                                            "                    ,dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblDLDocument.sPayMethod as payMethod\n" + 
                                                            "                    ,dbo.tblDLDocument.sSessionNumIDf as SessionNum\n" +                    
                                                            "                FROM [dbo].[tblDLTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblDLDocument\n" +
                                                            "                on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and sMatchTransSourceIDf != 'ARP' and dbo.tblDLDocument.dtmPosted > @LastUpdate and LEFT(dbo.tblDLTrans.sOrigSessionNumIDf,6) != 'BPSYNC' and dbo.tbldltrans.curAmount < 0 " + whereClause + "\n" +
                                                            "                group by dbo.tbldltrans.[dDocID], dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblDLDocument.sPayMethod, dbo.tblDLDocument.sSessionNumIDf\n" +
                                                            "                ) as A\n" +
                                                            "                UNION \n" +
                                                            "                Select * \n" +
                                                            "                from(SELECT dbo.tbltetrans.[dDocID] as pmtID\n" +
                                                            "                    ,dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                                                            "                    ,dbo.tblTEDocument.sSessionNumIDf as SessionNum\n" + 
                                                            "                FROM [dbo].[tblteTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblteDocument\n" +
                                                            "                on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and LEFT(dbo.tbltetrans.sOrigSessionNumIDf,6) = 'BPSYNC' " + whereClause + "\n" +
                                                            "                group by dbo.tbltetrans.[dDocID], dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblTEDocument.sPayMethod, tblTEDocument.sSessionNumIDf\n" +
                                                            "                ) as B\n" +
                                                            "                order by pmtReferenceNo asc, customerNo asc";
            
            System.out.println(testSQL);
            
            if(MainFrame.lastUpdate.isEmpty()){prevSync = false;}
            //Previously Synced?
            if(prevSync){
                //Previously synced, only bring new payments
                abilaPaymentsResultSet = stmt.executeQuery( "DECLARE @LastUpdate DATETIME\n" +
                                                            "SET @LastUpdate = '"+lastUpdate+"'\n" +
                                                            "\n" +
                                                            "Select * from(SELECT dbo.tbldltrans.[dDocID] as pmtID\n" +
                                                            "                    ,dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblDLDocument.sPayMethod as payMethod\n" + 
                                                            "                    ,dbo.tblDLDocument.sSessionNumIDf as SessionNum\n" +                    
                                                            "                FROM [dbo].[tblDLTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblDLDocument\n" +
                                                            "                on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and sMatchTransSourceIDf != 'ARP' and dbo.tblDLDocument.dtmPosted > @LastUpdate and LEFT(dbo.tblDLTrans.sOrigSessionNumIDf,6) != 'BPSYNC' and dbo.tbldltrans.[curAmount] < 0 " + whereClause + "\n" +
                                                            "                group by dbo.tbldltrans.[dDocID], dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblDLDocument.sPayMethod, dbo.tblDLDocument.sSessionNumIDf\n" +
                                                            "                ) as A\n" +
                                                            "                UNION \n" +
                                                            "                Select * \n" +
                                                            "                from(SELECT dbo.tbltetrans.[dDocID] as pmtID\n" +
                                                            "                    ,dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                                                            "                    ,dbo.tblTEDocument.sSessionNumIDf as SessionNum\n" + 
                                                            "                FROM [dbo].[tblteTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblteDocument\n" +
                                                            "                on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and LEFT(dbo.tbltetrans.sOrigSessionNumIDf,6) = 'BPSYNC' " + whereClause + "\n" +
                                                            "                group by dbo.tbltetrans.[dDocID], dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblTEDocument.sPayMethod, tblTEDocument.sSessionNumIDf\n" +
                                                            "                ) as B\n" +
                                                            "                order by pmtReferenceNo asc, customerNo asc"); 
            }else{
                //Not previously synced, so decide whether to sync all or only records related to transactions with balances
                if(syncTransWithBal){
                //Get Payments made to Invoices with open balances only
                abilaPaymentsResultSet = stmt.executeQuery( "select * from (Select * from(SELECT dbo.tbldltrans.[dDocID] as pmtID\n" +
                                                            "                ,dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                ,dbo.tblDLDocument.sPayMethod as payMethod\n" +
                                                            "            FROM [dbo].[tblDLTrans]\n" +
                                                            "            INNER JOIN\n" +
                                                            "            dbo.tblDLDocument\n" +
                                                            "            on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                                                            "            inner join dbo.tblARCustomer\n" +
                                                            "            on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "            where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and sMatchTransSourceIDf != 'ARP'" + appendedWhereClause + "\n" +
                                                            "            group by dbo.tbldltrans.[dDocID], dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblDLDocument.sPayMethod\n" +
                                                            "            ) as A\n" +
                                                            "            UNION \n" +
                                                            "            Select * \n" +
                                                            "            from(SELECT dbo.tbltetrans.[dDocID] as pmtID\n" +
                                                            "                ,dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "                ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                                                            "            FROM [dbo].[tblteTrans]\n" +
                                                            "            INNER JOIN\n" +
                                                            "            dbo.tblteDocument\n" +
                                                            "            on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                                                            "            inner join dbo.tblARCustomer\n" +
                                                            "            on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "            where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and LEFT(dbo.tbltetrans.sOrigSessionNumIDf,6) = 'BPSYNC' " + appendedWhereClause + "\n" +
                                                            "            group by dbo.tbltetrans.[dDocID], dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tbltetrans.sCodeIDf_1, dbo.tblTEDocument.sPayMethod\n" +
                                                            "            ) as B) as C\n" +
                                                            "			inner join\n" +
                                                            "			(SELECT [dDocID] from [tblAROpenDoc] where [tblAROpenDoc].curAmount != 0 and tblAROpenDoc.sDocNum != '<Prepay>' and tblAROpenDoc.sTransSourceIDf != 'ARP') As D\n" +
                                                            "			on C.appliedToID = D.dDocID\n" +
                                                            "            order by pmtReferenceNo asc, customerNo asc"); 
                }else{
                    //Get all Payments 
                    abilaPaymentsResultSet = stmt.executeQuery( "Select * from(SELECT dbo.tbldltrans.[dDocID] as pmtID\n" +
                                                                "                ,dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                                "                ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                                                                "                ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                                "                ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                                "                ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                                                                "                ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                                "                ,dbo.tblDLDocument.sPayMethod as payMethod\n" +
                                                                "            FROM [dbo].[tblDLTrans]\n" +
                                                                "            INNER JOIN\n" +
                                                                "            dbo.tblDLDocument\n" +
                                                                "            on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                                                                "            inner join dbo.tblARCustomer\n" +
                                                                "            on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                                "            where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and sMatchTransSourceIDf != 'ARP'" + appendedWhereClause + "\n" +
                                                                "            group by dbo.tbldltrans.[dDocID], dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblDLDocument.sPayMethod\n" +
                                                                "            ) as A\n" +
                                                                "            UNION \n" +
                                                                "            Select * \n" +
                                                                "            from(SELECT dbo.tbltetrans.[dDocID] as pmtID\n" +
                                                                "                ,dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                                "                ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                                                                "                ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                                "                ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                                "                ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                                                                "                ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                                "                ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                                                                "            FROM [dbo].[tblteTrans]\n" +
                                                                "            INNER JOIN\n" +
                                                                "            dbo.tblteDocument\n" +
                                                                "            on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                                                                "            inner join dbo.tblARCustomer\n" +
                                                                "            on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                                "            where sOrigTransSourceIDf in ('ARC','ARM') and sInvSrcCurrencyIDf != '' and LEFT(dbo.tbltetrans.sOrigSessionNumIDf,6) = 'BPSYNC' " + appendedWhereClause + "\n" +
                                                                "            group by dbo.tbltetrans.[dDocID], dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], dbo.tblARCustomer.sCustomerID, dbo.tblTEDocument.sPayMethod\n" +
                                                                "            ) as B\n" +
                                                                "            order by pmtReferenceNo asc, customerNo asc"); 
                }
            }
            
            int ctr = 0;
            String curRefNo = "";
            String curCusNo = "";
            String newRefNo = "";
            String newCusNo = "";
            ArrayList<Invoice> appliedInvoices = new ArrayList<Invoice>();
            ArrayList<String> appliedAmt = new ArrayList<String>();
            double paymentAmount = 0;
            String fund = "";
            String pmtMethod = "";
            String pmtSessionNum = "";
            
            String pmtdt = "";
            String id = "";
            
            String appliedToRef = "";
            String appliedToId = "";
            String applied = "";
            
            
            while(abilaPaymentsResultSet.next()){
                newRefNo = abilaPaymentsResultSet.getString("pmtReferenceNo");
                newCusNo = abilaPaymentsResultSet.getString("customerNo");
                //while(curRefNo == newRefNo && curCusNo == newCusNo)
                if(curRefNo.equals(newRefNo) && curCusNo.equals(newCusNo)){
                    
                    
                    //Don't need Payment Fields because they have already been assigned when the new Payment was identified                  
                    
                    
                    //Applied To Fields
                    appliedToRef = abilaPaymentsResultSet.getString("appliedToRefNo");
                    appliedToId = abilaPaymentsResultSet.getString("appliedToID");
                    applied = abilaPaymentsResultSet.getString("pmtAmount");
                    paymentAmount += Double.valueOf(applied);
                    
                    //Update applied Invocies and Amount                                     
                    Invoice inv = new Invoice(appliedToId, appliedToRef);
                    appliedInvoices.add(inv);
                    appliedAmt.add(applied);
                    ctr++;
                }else{
                    //Add previous payment to abilaPayments   
                    if(ctr != 0){
                        Payment pmt = new Payment(new ArrayList<Invoice>(appliedInvoices), new ArrayList<String>(appliedAmt), pmtdt, id, Double.toString(paymentAmount), curCusNo, curRefNo, fund, pmtMethod, pmtSessionNum);
                        MainFrame.abilaPayments.add(pmt);
                    }                    
                
                    //Set the new payment info to the current payment variables and clear previous values
                    curRefNo = newRefNo;
                    curCusNo = newCusNo;
                    paymentAmount = 0;
                    appliedInvoices.clear();
                    appliedAmt.clear();
                    pmtdt = abilaPaymentsResultSet.getString("pmtDate");
                    id = abilaPaymentsResultSet.getString("pmtId");
                    paymentAmount = Double.valueOf(abilaPaymentsResultSet.getString("pmtAmount"));
                    //fund = abilaPaymentsResultSet.getString("fund");
                    pmtMethod = abilaPaymentsResultSet.getString("payMethod");
                    pmtSessionNum = abilaPaymentsResultSet.getString("sessionNum");
                    appliedToRef = abilaPaymentsResultSet.getString("appliedToRefNo");
                    appliedToId = abilaPaymentsResultSet.getString("appliedToID");
                    applied = abilaPaymentsResultSet.getString("pmtAmount");
                    
                    //Add applied invoice and amount of new payment
                    
                    Invoice inv = new Invoice(appliedToId, appliedToRef);
                    appliedInvoices.add(inv);
                    appliedAmt.add(applied);
                    
                    
                    
                    ctr++;
                }
                
                
                
                //Payment p = new Invoice(invdt, dd, id, rn, cid, bal1, bal2, bal3, bc, bs, bz, qty, ip, it, idesc);
                 
                
                
            }
            
            if(ctr != 0){
                Payment pmt = new Payment(new ArrayList<Invoice>(appliedInvoices), new ArrayList<String>(appliedAmt), pmtdt, id, Double.toString(paymentAmount), curCusNo, curRefNo, fund, pmtMethod, pmtSessionNum);
                MainFrame.abilaPayments.add(pmt);
            }
            
            intermediateJTextArea.append("Retrieved " + MainFrame.abilaPayments.size() + " payments from Abila MIP Fund Accounting...\n");
            log.writeToFile("Retrieved " + MainFrame.abilaPayments.size() + " payments from Abila MIP Fund Accounting...\n");
            
        }catch (Exception e){
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
        }   
    }
    
    private static void orderPayments(ArrayList<Payment> payments) {

        Collections.sort(payments, new Comparator() {

            public int compare(Object o1, Object o2) {

                String rNo1 = ((Payment) o1).getReferenceNo();
                String rNo2 = ((Payment) o2).getReferenceNo();
                int sComp = rNo1.compareTo(rNo2);

                if (sComp != 0) {
                   return sComp;
                } else {
                   String cusID1 = ((Payment) o1).getCustomerNo();
                   String cusID2 = ((Payment) o2).getCustomerNo();
                   return cusID1.compareTo(cusID2);
                }
            }
        });
    }
    
    //Not used becasue the BP API was modified to return the invoice reference number when getting payments.
    public void getBPInvoiceReferenceNumbers(ArrayList<Payment> p){
        int invCnt;
        String xml = "";
        String cusID = "";
        String invID = "";
        String invRef = "";
        
        for (Payment pmt: p){
            cusID = pmt.getCustomerNo();
            invCnt = pmt.getAppliedTo().size();
            for(int i = 0; i < invCnt; i++){
                invID = pmt.getAppliedTo().get(i).getId();
                log.writeToFile("Invoice ID: " + invID + " - ");
                xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <invoiceinfo> <field>number</field> <where> <id>" + invID + "</id> <customerid>" + cusID + "</customerid> </where> </invoiceinfo> </biller> </request>";
                
                try {
                    invRef = pcgrf.performPostCall(xml);
                } catch (IOException ex) {
                    Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                } catch (SAXException ex) {
                    Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ParserConfigurationException ex) {
                    Logger.getLogger(MainFrame.class.getName()).log(Level.SEVERE, null, ex);
                }
                pmt.getAppliedTo().get(i).setReferenceNumber(invRef);
            }
        }
    } 
    
    private void comparePayments(ArrayList<Payment> a, ArrayList<Payment> b){
        boolean keepLooking = true;
        //boolean equal = false;
        int bpPmtCtr = 0;
        int abilaPmtCtr = 0;
        
        String refA = "";
        String cusIDA = "";
        String pmtAmtA = "";
        
        
        String refB = "";
        String cusIDB = "";
        String pmtAmtB = "";
        
        while(abilaPmtCtr < a.size() && bpPmtCtr < b.size()){
            refA = a.get(abilaPmtCtr).getReferenceNo();
            cusIDA = a.get(abilaPmtCtr).getCustomerNo();
            pmtAmtA = a.get(abilaPmtCtr).getPmtAmount();
            
            
        
            refB = b.get(bpPmtCtr).getReferenceNo();
            cusIDB = b.get(bpPmtCtr).getCustomerNo();
            pmtAmtB = b.get(abilaPmtCtr).getPmtAmount();
                        
            int compareRefAtoB = refA.compareTo(refB);
            int compareCusAtoB = cusIDA.compareTo(cusIDB);
            int comparePmtAmtAtoB = pmtAmtA.compareTo(pmtAmtB);
            boolean compareAppliedAtoB = comparePmtApplied(a.get(abilaPmtCtr), b.get(bpPmtCtr));
            
            //boolean test = b.get(bpPmtCtr).getPmtMethod().equals("AbilaPMT");
            
            
            if(compareRefAtoB == 0){
                if(compareCusAtoB == 0){
                    //The Payment is in both Bill and Pay and Abila. 
                    if(comparePmtAmtAtoB == 0 && compareAppliedAtoB == true){ //check if payment amount or applied amounts are the same
                        //payments are exactly the same an no change is needed
                        bpPmtCtr++;
                        abilaPmtCtr++;
                    }else{
                        
                    }
                    
                }else if(compareCusAtoB < 0){
                    //Abila's Payment is not in Bill and Pay.
                    if(a.get(abilaPmtCtr).getPmtSessionNum().startsWith("BPSYNC")){ 
                        abilaPmtCtr++;
                    }else{
                        MainFrame.paymentsToAddToBillandPay.add(a.get(abilaPmtCtr));
                        abilaPmtCtr++;
                    }
                }else{
                    //Bill and Pay Payment is not in Abila.
                    if(b.get(bpPmtCtr).getPmtMethod().equals("AbilaPMT")){
                        //This is an Abila Payment that did not get returned by the getAbilaPayments function, probably because of the date last sync limited the record. This should be resolved eventually by limiting the payments retrieved from Bill and Pay by the last date sync.
                        bpPmtCtr++;
                    }else if(!b.get(bpPmtCtr).getPmtMethod().equals("BP Cash-Check")){
                        MainFrame.paymentsToAddToAbila.add(b.get(bpPmtCtr));
                        bpPmtCtr++;
                    }

                }                
            }else if(compareRefAtoB < 0){
                //Abila's Payment is not in Bill and Pay.
                //Old if statement: if(b.get(bpPmtCtr).getPmtMethod().equals("Bill and Pay")) 
                if(a.get(abilaPmtCtr).getPmtSessionNum().startsWith("BPSYNC")){
                    abilaPmtCtr++;
                }else{
                    MainFrame.paymentsToAddToBillandPay.add(a.get(abilaPmtCtr));
                    abilaPmtCtr++;
                }
            }else{
                //Bill and Pay Payment is not in Abila.

                if(b.get(bpPmtCtr).getPmtMethod().equals("AbilaPMT") || b.get(bpPmtCtr).getPmtMethod().equals("BP Cash-Check")){
                //This is an Abila Payment that did not get returned by the getAbilaPayments function, probably because of the date last sync limited the record. This should be resolved eventually by limiting the payments retrieved from Bill and Pay by the last date sync.                    
                    bpPmtCtr++;
                }else{
                    MainFrame.paymentsToAddToAbila.add(b.get(bpPmtCtr));
                    bpPmtCtr++;
                }
            }            
        }
        
        if(abilaPmtCtr < a.size()){
            for(int i = abilaPmtCtr; i < a.size(); i++){
                String PmtSessionNum = a.get(i).getPmtSessionNum();
                if(!a.get(i).getPmtSessionNum().startsWith("BPSYNC")){
                    MainFrame.paymentsToAddToBillandPay.add(a.get(i));
                }
            }
        }else if(bpPmtCtr < b.size()){
            for(int j = bpPmtCtr; j < b.size(); j++){
                if(!b.get(j).getPmtMethod().equals("AbilaPMT") && !b.get(j).getPmtMethod().equals("BP Cash-Check")){
                   MainFrame.paymentsToAddToAbila.add(b.get(j)); 
                }                
            }
        }
        
        
        intermediateJTextArea.append("A total of " + MainFrame.paymentsToAddToAbila.size() + " Payments need to be added to Abila...\n");        
        intermediateJTextArea.append("A total of " + MainFrame.paymentsToAddToBillandPay.size() + " Payments need to be added to Bill and Pay...\n");   
    }
    
    private boolean comparePmtApplied(Payment abilaPayment, Payment bpPayment){
        //compare total applied
        if(abilaPayment.getAppliedTo().size() == abilaPayment.getAppliedTo().size()){ //first check if total applied invoices is the same
            for(int i = 0; i < abilaPayment.getAppliedTo().size(); i++){//iterate through all applied invoices and amounts
                if(abilaPayment.getAppliedTo().get(i).getReferenceNumber().compareTo(bpPayment.getAppliedTo().get(i).getReferenceNumber()) == 0){
                    if(abilaPayment.getAppliedAmount().get(i).compareTo(bpPayment.getAppliedAmount().get(i)) == 0){
                        //go to the next invoice because this one is the same.
                    }else{
                        return false;
                    }
                }else{
                    return false;
                }
            }
        }else{
            return false;      
        }
        return true;
    }
    
    private String buildAbilaInsertSession(String transType, String ssnDesc){
        String ssnTransSource = transType;
        String ssnDate = getSsnDate();
        String ssnDescription = ssnDesc;
        String dateLastUpdated = getTransDate();
        String ssnID = getAbilaSessionID();
        ARCSessionID = ssnID;
        
        String SQL = "exec spInsertSession @sTransSourceID=N'"+ ssnTransSource + "',@dtmSessionDate='" + ssnDate + "',@sDescription=N'" + ssnDescription + "',@sStatus=N'BS',@dtmLastUpdated='" + dateLastUpdated + "',@ysnMarkedPost=0,@dtmNewCheckDate='1899-12-28 00:00:00',@dtmAPICheckDate='2016-02-10 00:00:00',@ysnVCUseOriginalDate=1,@sSessionNumID=N'" + ssnID + "',@sBudgetVersion=N'',@sCurrencyIDf=N'USD',@sRateType=N'',@dtmRevalDate='1899-12-28 00:00:00',@sRevalRateType=N'',@ysnAutoCloseRev=0,@dtmAutoCloseRevDate='1899-12-28 00:00:00',@sCreditCardID=N'',@ysnARPinARC=0,@sCodeIDf_0=''";   // this last part is for newer version of Abila.      
        return SQL;
    }    
    
    private String getSsnDate(){
        DateFormat df = new SimpleDateFormat("YYYY-MM-dd");
        Date today = Calendar.getInstance().getTime();
        String formattedToday = df.format(today);
        String ssnTransDate = formattedToday + " 00:00:00";
        return ssnTransDate;
    }
    
    private String getTransDate(){
        DateFormat df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date now = Calendar.getInstance().getTime();
        String formattedNow = df.format(now);
        String transDate = formattedNow;
        //long timeInMillis = now.getTime();
        return transDate;
    }
    
    private String getAbilaSessionID(){
        DateFormat df = new SimpleDateFormat("YYMMddHHmmss");
        Date now = Calendar.getInstance().getTime();
        String formattedNow = df.format(now);
        String sessionID = "BPSYNC" + formattedNow;
        return sessionID;
    }
     
    public void insertAbilaDocsandLines(ArrayList<Payment> paymentsToAdd){
        //Create Payment variables needed.
        String sql;
        String pmtRef = "";
        String pmtID = "";
        String cust = "";
        String custName = "";
        String pmtDate = "";
        String pmtDesc = "Bill and Pay Receipt";
        String PmtAmount = "";
        String lastUpdated = "";
        
        //Create appliedTo and appliedAmount variables needed.
        String invoiceAppliedAmount = "";
        String origDocNum = "";
        String matchDocID = "";
        String matchRefNo = "";
        ArrayList<InvoiceBalanceDetail> invBalDetail = new ArrayList<InvoiceBalanceDetail>();
        
        
        
        //Loop through each Payment and Applied Invoices and Amounts within to create Document and Transaction Lines in Abila.
        forCount = 1;
        for(Payment p : paymentsToAdd){
            if(!isCancelled()){ //Needs the else section
                pmtRef = p.getReferenceNo();
                
                //Sleep for one second so that different pmtID can be generated. 
                try {
                    
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    Logger.getLogger(BackgroundSync.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                //Create value to pass as ctrDocID to spTEDocInsert stored procedure.
                Calendar cal = Calendar.getInstance();        
                final long MILLIS_IN_DAY = 1000L * 60L * 60L * 24L;

                final Calendar startOfTime = Calendar.getInstance();
                startOfTime.setTimeZone(TimeZone.getTimeZone("UTC"));
                startOfTime.clear();
                startOfTime.set(1900, 0, 1, 0, 0, 0);

                final Calendar myDate = Calendar.getInstance();
                myDate.setTimeZone(TimeZone.getTimeZone("UTC"));
                myDate.clear();
                myDate.set(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND)); 

                final long diff = myDate.getTimeInMillis() - startOfTime.getTimeInMillis() + (2 * MILLIS_IN_DAY);
                final double pmtIDDbl = (double) diff / (double) MILLIS_IN_DAY;
                //pmtID = Double.toString(pmtIDDbl);
                pmtID = left(String.valueOf(pmtIDDbl),15);

                
                
                
                /**long now = Calendar.getInstance().getTime().getTime();
                String nowString = String.valueOf(now);  
                pmtID = left(nowString,5) + "." + right(nowString,length(nowString) - 5);**/

                //Get Payment object variables.
                cust = p.getCustomerNo();
                custName = p.getCustomerName();
                
                pmtDate = p.getPmtDate();
                PmtAmount = p.getPmtAmount();
                
                String test = p.getBpLastUpdate();
                
                if(lastUpdated.compareTo(p.getBpLastUpdate()) < 0){
                    lastUpdated = p.getBpLastUpdate();
                }

                MainFrame.progressBarLabels.setText("Adding Payment "+forCount+" of "+paymentsToAdd.size()+" to Abila");
                forCount = forCount + 1;
                value = value + 1;
                double percentages = (value/total) * 100;
                int percentage = (int) percentages;
                setProgress(percentage); 
                //Create Document in Abila
                sql = "exec spTEDocInsert @ctrDocID=" + pmtID + ",@sSessionNumIDf=N'" +  ARCSessionID + "',@sTransSourceIDf=N'ARC',@sDocNum=N'" + pmtRef + "',@sDepositNum=N'',@sAddressee=N'" + custName + "',@sAddress=N'',@sDescription=N'" + pmtDesc + "',@sPlayerNumIDf=N'" + cust + "',@sPlayerTypeIDf=N'C',@sAddressID=N'',@dtmDocDate='" + pmtDate + "',@dtmDueDate='1899-12-28 00:00:00',@dtmPosted='1899-12-28 00:00:00',@curAmount=" + PmtAmount + ",@curSrcAmount=" + PmtAmount + ",@sCurrencyIDf=N'USD',@sRateType=N'',@ysnInvoiceAdjustment=0,@dDocLinkID=0,@sCreatedBy=N'ADMIN',@sCreatedAt=N'EBCO2',@dtmCreated='" + pmtDate + "',@sModifiedBy=N'ADMIN',@sModifiedAt=N'EBCO2',@dtmModified='" + pmtDate + "',@ysnLiquidateENC=0,@ysnEPay=0,@ysnPrepayment=0,@sBudgetVersion=N'',@s1099TypeID=N'',@sCreditType=N'',@sPayMethod=N'Bill and Pay',@sCCAType=N'',@sCCAHolder=N''";
                
                if(abilaVersion.compareTo("17") > 0){
                    sql += ",@ysnAutoRevInv=0,@dtmAutoRevInv='1899-12-28 00:00:00'";
                }

                executeAbilaSQLStatement(sql);
                int orderID = 1;

                //Loop through each invoice the payment was applied to 
                for(int i = 0; i < p.getAppliedAmount().size(); i++){
                    //Set Customer ID at the AppliedTo level; which is not set before.
                    p.getAppliedTo().get(i).setCustomerID(cust);
                    
                    //Get appliedTo and appliedAmount values per each Payment
                    invoiceAppliedAmount = p.getAppliedAmount().get(i);
                    origDocNum = p.getAppliedTo().get(i).getReferenceNumber();
                    matchDocID = p.getAppliedTo().get(i).getId();
                    matchRefNo = p.getAppliedTo().get(i).getReferenceNumber();                
                    getInvoiceBalanceDetail(p.getAppliedTo().get(i), invBalDetail);                    

                    //Create variables for multiple Invoice lines to apply payment to and maintain cummulative for last line rounding error prevention.
                    double invoiceLineAppliedAmt = 0.00;
                    double cummInvoiceLineAppliedAmt = 0.00;

                    for(int j = 0; j < invBalDetail.size(); j++){
                        //Calculate Invoice applied amount. Using proportional payment in case of partial payment.
                        if(invBalDetail.size() == 1){
                            invoiceLineAppliedAmt = Double.parseDouble(invoiceAppliedAmount);
                        }else if(i == invBalDetail.size() - 1){
                            invoiceLineAppliedAmt = cummInvoiceLineAppliedAmt - Double.parseDouble(invoiceAppliedAmount);
                        }else{
                            //This is the scenario when the Invoice has multiple distributions with balances. Need to be changed to incorporate when some of the balances are negative; the preliminary strategy for this is to apply the negative amounts proportionately to positive amouts and then apply the payment proportionately to the positive balances remaining.
                            double applyPercentage = Double.parseDouble(invBalDetail.get(j).getGlBalance())/Double.parseDouble(invoiceAppliedAmount);
                            invoiceLineAppliedAmt = Double.parseDouble(invoiceAppliedAmount) * applyPercentage;
                            cummInvoiceLineAppliedAmt = cummInvoiceLineAppliedAmt + invoiceLineAppliedAmt;
                        }
                        
                        //Build Account Code section of Invoice Line SQL statement
                        String invLineSQL = "";
                        for(int k = 0; k < invBalDetail.get(j).getAccountDistribution().size(); k++){
                            invLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N'" + invBalDetail.get(j).getAccountDistribution().get(k).getCodeID() + "'";
                        }
                        
                        
                        
                        
                        //Invoice to be paid line.
                        sql = "exec spTEDocTELineInsert @sOrigDocNum=N'" + pmtRef + "',@sOrigSessionNumIDf=N'" +  ARCSessionID + "',@sOrigTransSourceIDf=N'ARC',@nOrderID=" + orderID + ",@dtmPostTo='" + pmtDate + "',@sEntryType=N'N',@sCashTransType=N'',@sDescription=N'" + pmtDesc + "',@s1099BoxNum=N'N/A',@dMatchDocIDf=" + matchDocID + ",@sMatchDocNum=N'" + matchRefNo + "',@sMatchSessionNumIDf=N'" + invBalDetail.get(j).getSessionID() + "',@sMatchTransSourceIDf=N'" + invBalDetail.get(j).getTransSource() + "',@ysnExchangeRateLocked=0,@nLineLinkID=0,@nEncLineLinkID=0,@dDocId=" + pmtID + invLineSQL + ",@curAmount=-" + Double.toString(invoiceLineAppliedAmt) + ",@curSrcAmount=-" + Double.toString(invoiceLineAppliedAmt) + ",@decExchangeRate=1,@sCurrencyIDf=N'USD',@curInvSrcAmount=-" + Double.toString(invoiceLineAppliedAmt) + ",@sInvSrcCurrencyIDf=N'USD',@decInvSrcExchangeRate=1";

                        executeAbilaSQLStatement(sql);
                        orderID++;
                        
                        //Offset GL line.
                        if(invBalDetail.get(j).getOffsetGL().equals("")){
                            //There's no Accout Offset defined. No actions needed.
                            
                            
                            //Should I notify the user that they need to cofigure this? If so, include code here.
                            String missingOffsetGLs = "";
                            if(!MainFrame.fundSegment.equals("")){
                                missingOffsetGLs = "Fund: " + invBalDetail.get(j).getAccountDistribution().get(MainFrame.fundSegmentIndex).getCodeID() + " | GL: " + invBalDetail.get(j).getAccountDistribution().get(MainFrame.glSegmentIndex).getCodeID();
                                
                            }else{
                                missingOffsetGLs = "GL: " + invBalDetail.get(j).getAccountDistribution().get(MainFrame.glSegmentIndex).getCodeID();
                            }
                            
                            intermediateJTextArea.append("There's no offset account set for Accounts Receivable Receipt (ARC) transactions for Session ID: " + ARCSessionID + " | Payment No.: " + pmtRef + " | " + missingOffsetGLs + ". You will need to create the missing offset accouts or enter them manually for each transaction.\n");;
                            log.writeToFile("There's no offset account set for Accounts Receivable Receipt (ARC) transactions for Session ID: " + ARCSessionID + " | Payment No.: " + pmtRef + " | " + missingOffsetGLs + ". You will need to create the missing offset accouts or enter them manually for each transaction.");
                            System.out.println("There's no offset account set for Accounts Receivable Receipt (ARC) transactions for Session ID: " + ARCSessionID + " | Payment No.: " + pmtRef + " | " + missingOffsetGLs + ". You will need to create the missing offset accouts or enter them manually for each transaction.");
                            
                            
                        }else{
                            //Build offset Account Code section of Invoice Line SQL statement
                            String offsetLineSQL = "";
                            ArrayList<Integer> reqSegments = null;
                            int nextReqSegmentIndex = 0;
                            int nextReqSegment = -1;
                            
                            boolean requiredSegmentsHasValues = !MainFrame.requiredGLAssigments.isEmpty();
                            if(requiredSegmentsHasValues){
                                String glSegmentCode = invBalDetail.get(j).getOffsetGL();
                                int mainFrameLocation = getGLRequiredSegments(glSegmentCode);
                                if(mainFrameLocation >= 0){
                                    reqSegments = new ArrayList<Integer>(MainFrame.requiredGLAssigments.get(mainFrameLocation).getReqSegments());
                                    nextReqSegment = reqSegments.get(nextReqSegmentIndex);                                    
                                }                                
                            }
                            
                            for(int k = 0; k < invBalDetail.get(j).getAccountDistribution().size(); k++){
                                if(MainFrame.accountSegments.get(k).getType().equals("GL")){
                                    offsetLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N'" + invBalDetail.get(j).getOffsetGL() + "'";                                
                                }else if(MainFrame.accountSegments.get(k).getType().equals("FUND")){
                                    offsetLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N'" + invBalDetail.get(j).getOffsetFund() + "'"; 
                                }else if(MainFrame.accountSegments.get(k).getType().equals("BAL")){
                                    offsetLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N'" + invBalDetail.get(j).getAccountDistribution().get(k).getCodeID() + "'";
                                }else if(MainFrame.accountSegments.get(k).getType().equals("NBAL") && k == nextReqSegment){
                                    offsetLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N'" + invBalDetail.get(j).getAccountDistribution().get(k).getCodeID() + "'";
                                    if((nextReqSegmentIndex + 1) < reqSegments.size()){
                                        nextReqSegmentIndex++;
                                        nextReqSegment = reqSegments.get(nextReqSegmentIndex);
                                    }                                    
                                }else{
                                    offsetLineSQL += ",@"+ invBalDetail.get(j).getAccountDistribution().get(k).getSegmentID() + "=N''";
                                }
                            }
                        
                            //Build SQL String for Offset Account Line
                            sql = "exec spTEDocTELineInsert @sOrigDocNum=N'" + pmtRef + "',@sOrigSessionNumIDf=N'" +  ARCSessionID + "',@sOrigTransSourceIDf=N'ARC',@nOrderID=" + orderID + ",@dtmPostTo='" + pmtDate + "',@sEntryType=N'N',@sCashTransType=N'CR',@sDescription=N'" + pmtDesc + "',@s1099BoxNum=N'N/A',@dMatchDocIDf=" + matchDocID + ",@sMatchDocNum=N'" + matchRefNo + "',@sMatchSessionNumIDf=N'" + invBalDetail.get(j).getSessionID() + "',@sMatchTransSourceIDf=N'" + invBalDetail.get(j).getTransSource() + "',@ysnExchangeRateLocked=0,@nLineLinkID=0,@nEncLineLinkID=0,@dDocId=" + pmtID + offsetLineSQL + ",@curAmount=" + Double.toString(invoiceLineAppliedAmt) + ",@curSrcAmount=" + Double.toString(invoiceLineAppliedAmt) + ",@decExchangeRate=1,@sCurrencyIDf=N'USD',@curInvSrcAmount=0.00,@sInvSrcCurrencyIDf=N'',@decInvSrcExchangeRate=1";

                            executeAbilaSQLStatement(sql);
                            orderID++;
                            cummInvoiceLineAppliedAmt += invoiceLineAppliedAmt;
                        }
                        

                        //Need to add code in case there are intrafund payables/receivables.
                    }
                }
            } 
        }
        
        setLastBPPmtRetrieveDate(lastUpdated);
        
    }
    
    public int getGLRequiredSegments(String glCode){
        boolean keepLooking = true;
        int ctr = 0;
        
        while(keepLooking && ctr < MainFrame.requiredGLAssigments.size()){
            String compareGLCode = MainFrame.requiredGLAssigments.get(ctr).getGlCode();
            //ctr++;
            if(glCode.compareTo(compareGLCode) < 0){
                //the value we are looking for is smaller than the current Mainframe list. Since the Mainframe List is in ascending order, the value for GL beign searched for is not in the list. Stop looking and return null.
                return -1;
            }else if(glCode.compareTo(compareGLCode) == 0){
                //Thevalue was found. REturn the value and stop looking.
                return ctr;
            }else if(glCode.compareTo(compareGLCode) > 0){
                //the value we aare looking for is greater than the current mainframe list. Increase ctr to get the next value to compare.
                ctr++;
            }
        }
        
        return -1;
    }
    
    public void executeAbilaSQLStatement(String sql){
        getSQLProperties();
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);         
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql); 
        }catch (Exception e){
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, e);
        }   
    }
    
    public void getInvoiceBalanceDetail(Invoice inv, ArrayList<InvoiceBalanceDetail> invBalDetail){
        invBalDetail.clear();
        getSQLProperties();
        ArrayList<AccountCode> accountDistribution = new ArrayList<AccountCode>();
        
        if(inv.getReferenceNumber() == "1450"){
            System.out.println("1450");
        }
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
         
            Statement stmt = connection.createStatement();
            ResultSet abilaInvBalDetailResultSet;  
            
            //Build the Accounting Segments SQL String
            String accSegSQL = "";
            for(int i=0;i < MainFrame.accountSegments.size();i++){
                accSegSQL +=  ",sCodeIDf_" + i;
                
            } 
            
            String test = "SELECT dMatchDocIDf\n" +
                                                            "		,sMatchDocNum\n" +
                                                            "		,sMatchSessionNumIDf\n" +
                                                            "		,tblAROpenDoc.sTransSourceIDf\n" +
                                                            "		,tblDLDocument.sPlayerNumIDf \n" +
                                                            "		,sum(tblDLTrans.[curSrcAmount]) Balance\n" + accSegSQL + "\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetGL),'') as 'sOffsetGL'\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetFund),'') as 'sOffsetFund'\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetIF),'') as 'sOffsetIF'\n" + 
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sTriggerIF),'') as 'sTriggerIF'\n" + 
                                                            "	FROM [dbo].[tblDLTrans]\n" +
                                                            "	join dbo.tblDLDocument\n" +
                                                            "	on tblDLDocument.ctrDocID = tblDLTrans.dMatchDocIDf \n" +
                                                            "	left join dbo.tblOffsetAssign\n" +
                                                            "	on tblDLTrans." + MainFrame.glSegment + " = tblOffsetAssign.sTriggerGLID and tblDLTrans." + MainFrame.fundSegment + " = tblOffsetAssign.sTriggerFundID and tblOffsetAssign.sTransSourceID = 'ARC'\n" +
                                                            "	join dbo.tblAROpenDoc\n" +
                                                            "	on tblDLTrans.dMatchDocIDf = dbo.tblAROpenDoc.dDocID\n" +
                                                            "	where  sInvSrcCurrencyIDf != '' and tblDLDocument.sPlayerNumIDf = '" + inv.getCustomerID() + "' and sMatchDocNum = '" + inv.getReferenceNumber() + "'\n" +
                                                            "	group by dMatchDocIDf\n" +
                                                            "		,sMatchDocNum\n" +
                                                            "		,sMatchSessionNumIDf\n" +
                                                            "		,tblAROpenDoc.sTransSourceIDf\n" +
                                                            "		,tblDLDocument.sPlayerNumIDf\n" + accSegSQL + "\n" +
                                                            "		,tblOffsetAssign.sOffsetGL\n" +
                                                            "		,tblOffsetAssign.sOffsetFund\n" +
                                                            "		,tblOffsetAssign.sOffsetIF\n" +
                                                            "		,tblOffsetAssign.sTriggerIF";
            
            //Execute the Query
            abilaInvBalDetailResultSet = stmt.executeQuery("SELECT dMatchDocIDf\n" +
                                                            "		,sMatchDocNum\n" +
                                                            "		,sMatchSessionNumIDf\n" +
                                                            "		,tblAROpenDoc.sTransSourceIDf\n" +
                                                            "		,tblDLDocument.sPlayerNumIDf \n" +
                                                            "		,sum(tblDLTrans.[curSrcAmount]) Balance\n" + accSegSQL + "\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetGL),'') as 'sOffsetGL'\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetFund),'') as 'sOffsetFund'\n" +
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sOffsetIF),'') as 'sOffsetIF'\n" + 
                                                            "           ,ISNULL(CONVERT(varchar(50),tblOffsetAssign.sTriggerIF),'') as 'sTriggerIF'\n" + 
                                                            "	FROM [dbo].[tblDLTrans]\n" +
                                                            "	join dbo.tblDLDocument\n" +
                                                            "	on tblDLDocument.ctrDocID = tblDLTrans.dMatchDocIDf \n" +
                                                            "	left join dbo.tblOffsetAssign\n" +
                                                            "	on tblDLTrans." + MainFrame.glSegment + " = tblOffsetAssign.sTriggerGLID and tblDLTrans." + MainFrame.fundSegment + " = tblOffsetAssign.sTriggerFundID and tblOffsetAssign.sTransSourceID = 'ARC'\n" +
                                                            "	join dbo.tblAROpenDoc\n" +
                                                            "	on tblDLTrans.dMatchDocIDf = dbo.tblAROpenDoc.dDocID\n" +
                                                            "	where  sInvSrcCurrencyIDf != '' and tblDLDocument.sPlayerNumIDf = '" + inv.getCustomerID() + "' and sMatchDocNum = '" + inv.getReferenceNumber() + "'\n" +
                                                            "	group by dMatchDocIDf\n" +
                                                            "		,sMatchDocNum\n" +
                                                            "		,sMatchSessionNumIDf\n" +
                                                            "		,tblAROpenDoc.sTransSourceIDf\n" +
                                                            "		,tblDLDocument.sPlayerNumIDf\n" + accSegSQL + "\n" +
                                                            "		,tblOffsetAssign.sOffsetGL\n" +
                                                            "		,tblOffsetAssign.sOffsetFund\n" +
                                                            "		,tblOffsetAssign.sOffsetIF\n" +
                                                            "		,tblOffsetAssign.sTriggerIF");
            
            //Read Results, create invoice object and add to Invoice Array
            while(abilaInvBalDetailResultSet.next()){
                accountDistribution.clear();
                String docID = abilaInvBalDetailResultSet.getString("dMatchDocIDf");
                String refNo = abilaInvBalDetailResultSet.getString("sMatchDocNum");
                String sessionNo = abilaInvBalDetailResultSet.getString("sMatchSessionNumIDf");
                String transSource = abilaInvBalDetailResultSet.getString("sTransSourceIDf");
                String cusNo = abilaInvBalDetailResultSet.getString("sPlayerNumIDf");
                String balance = abilaInvBalDetailResultSet.getString("Balance");
                
                for(int i=0;i < MainFrame.accountSegments.size();i++){
                    String code = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_" + i));
                    AccountCode ac = new AccountCode("sCodeIDf_" + i, code);                  
                    accountDistribution.add(ac);                  
                }
                
                String code0 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_0"));
                String code1 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_1"));
                String code2 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_2"));
                String code3 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_3"));
                String code4 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_4"));
                String code5 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_5"));
                String code6 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_6"));
                String code7 = trim(abilaInvBalDetailResultSet.getString("sCodeIDf_7"));
                String offsetGL = trim(abilaInvBalDetailResultSet.getString("sOffsetGL"));
                String offsetFund = trim(abilaInvBalDetailResultSet.getString("sOffsetFund"));
                String offsetIF = trim(abilaInvBalDetailResultSet.getString("sOffsetIF"));
                String triggerIF = trim(abilaInvBalDetailResultSet.getString("sTriggerIF"));                
                
                
                ArrayList<AccountCode> accountDistributionCopy = new ArrayList<AccountCode>(accountDistribution);
                InvoiceBalanceDetail invD = new InvoiceBalanceDetail(docID, refNo, sessionNo, transSource, cusNo, balance, accountDistributionCopy, code0, code1, code2, code3, code4, code5, code6, code7, offsetGL, offsetFund, offsetIF, triggerIF);                
                
                invBalDetail.add(invD);    
            }
            
        }catch (Exception e){
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, e);
        }        
    }
    
    public void insertAbilaSession(String sql){
        
        //updateProgress("Creating Abila ARC session...");
        
        getSQLProperties();
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);         
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql); 
        }catch (Exception e){
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, e);
        }   
    }
    
    private String buildAddPaymentToBPXML(Payment p){
        
        
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <paymentadd> ";
        
        
        xml += "<id>" + p.getId() + "</id> <customer> <id>" + p.getCustomerNo() + "</id> </customer> <date>" + p.getPmtDate() + "</date> <amount>" + p.getPmtAmount() + "</amount> <referencenumber>" + p.getReferenceNo() + "</referencenumber> <method>AbilaPMT</method> <appliedto> ";
        
        for(int i = 0; i < p.getAppliedTo().size(); i++){
            xml += "<invoice> <id>" + p.getAppliedTo().get(i).getId() + "</id> <amount>" + p.getAppliedAmount().get(i) + "</amount> </invoice> ";
        }
                
        
        xml += "</appliedto> </paymentadd> </biller> </request>";
        
        System.out.println(xml);
        
        //intermediateJTextArea.append(xml);
        return xml;
    }
    
    private String buildSendInvoiceEmail(Invoice inv){
        
        
        String xml = "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+billerID+"</id> <password>"+billerPassword+"</password> </authenticate> <invoicesend> <id>" + inv.getId() + "</id> ";
        
        xml += "</invoicesend> </biller> </request>";
        
        System.out.println(xml);        
        
        //intermediateJTextArea.append(xml);
        return xml;
    }
    
    private void insertReturnedPayments(String customerID, String paymentReferenceNo, String paymentAmount){
        Double origPmtAmountMod = 0.00;
        String origCustomerName = "";
        String origPmtDate = "";
        String pmtDesc = "Bill and Pay Returned Receipt";
        ArrayList<String> sqlStatements = new ArrayList<String>();
        boolean stop = false;
        returnedPaymentsSessionCreated = false;
        
        //Sleep for one second so that different pmtID can be generated. 
        try {

            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ex) {
            Logger.getLogger(BackgroundSync.class.getName()).log(Level.SEVERE, null, ex);
        }                
                
        //Create Payment ID
        Calendar cal = Calendar.getInstance();        
        final long MILLIS_IN_DAY = 1000L * 60L * 60L * 24L;

        final Calendar startOfTime = Calendar.getInstance();
        startOfTime.setTimeZone(TimeZone.getTimeZone("UTC"));
        startOfTime.clear();
        startOfTime.set(1900, 0, 1, 0, 0, 0);

        final Calendar myDate = Calendar.getInstance();
        myDate.setTimeZone(TimeZone.getTimeZone("UTC"));
        myDate.clear();
        myDate.set(cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),cal.get(Calendar.HOUR_OF_DAY),cal.get(Calendar.MINUTE),cal.get(Calendar.SECOND)); 

        final long diff = myDate.getTimeInMillis() - startOfTime.getTimeInMillis() + (2 * MILLIS_IN_DAY);
        final double pmtIDDbl = (double) diff / (double) MILLIS_IN_DAY;
        //pmtID = Double.toString(pmtIDDbl);
        String pmtID = left(String.valueOf(pmtIDDbl),15);
        
        /**long now = Calendar.getInstance().getTime().getTime();
        String nowString = String.valueOf(now);       
        String pmtID = left(nowString,5) + "." + right(nowString,length(nowString) - 5);**/
        
        
        getSQLProperties();
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
         
            Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            
            ResultSet abilaOriginalPaymentDetail;  
            
            //Build the Accounting Segments SQL String
            String accSegSQL = "";
            String accSegSQLGroupBy = "";
            for(int i=0;i < MainFrame.accountSegments.size();i++){
                accSegSQL +=  ",sCodeIDf_" + i;
                accSegSQL += " as " + "sCodeIDf_" + i;
                accSegSQLGroupBy +=  ",sCodeIDf_" + i;
            }
            
            String test =   "DECLARE @customerID VARCHAR(30)\n" +
                            "DECLARE @paymentNumber VARCHAR(30)\n" +
                            "SET @customerID = '" + customerID + "'\n" +
                            "SET @paymentNumber = '" + paymentReferenceNo + "'\n" +
                            "\n" +
                            "Select ROW_NUMBER() OVER (order by pmtReferenceNo) as 'Order',  pmtReferenceNo, pmtDate, sum(cast(round(pmtAmount,2) as numeric(36,2)) * -1) as pmtLineAmount, sum(cast(round(pmtTotal2,2) as numeric(36,2))) as pmtTotal, appliedToRefNo, appliedToID, appliedToSessionID, customerNo, customerName, payMethod, currency, cashTransType, matchTransSource, sum(cast(round(curInvSrcAmount,2) as numeric(36,2))) as curInvSrcAmount, invSrcCurrency" + accSegSQLGroupBy + " from (Select *  from(SELECT dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                            "                    ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                            "                    ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                            "                    ,(select sum(curAmount) from tblDLDocument where sDocNum = @paymentNumber and sPlayerNumIDf = @customerID and sTransSourceIDf = 'ARC') as pmtTotal2\n" +  
                            "                    ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                            "                    ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                            "			 ,dbo.tbldltrans.sMatchSessionNumIDf as appliedToSessionID\n" +
                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                            "                    ,dbo.tblARCustomer.sName as customerName\n" +
                            "                    ,dbo.tblDLDocument.sPayMethod as payMethod\n" + 
                            "                    ,dbo.tblDLtrans.sCurrencyIDf as currency\n" + accSegSQL + " \n" +
                            "                    ,dbo.tblDLtrans.sCashTransType as cashTransType\n" +
                            "                    ,dbo.tblDLtrans.sMatchTransSourceIDf as matchTransSource\n" +
                            "                    ,dbo.tblDLtrans.curInvSrcAmount as curInvSrcAmount\n" +
                            "	   		 ,dbo.tblDLtrans.sInvSrcCurrencyIDf as invSrcCurrency\n" +
                            "FROM [dbo].[tblDLTrans]\n" +
                            "                INNER JOIN\n" +
                            "                dbo.tblDLDocument\n" +
                            "                on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                            "                inner join dbo.tblARCustomer\n" +
                            "                on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                            "                where sOrigTransSourceIDf = 'ARC' and dbo.tbldltrans.[sOrigDocNum] = @paymentNumber and dbo.tblARCustomer.sCustomerID = @customerID " + appendedWhereClause + "\n" +
                            "                group by dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], sMatchSessionNumIDf, dbo.tblARCustomer.sCustomerID, dbo.tblARCustomer.sName, dbo.tblDLDocument.sPayMethod, dbo.tblDLtrans.sCurrencyIDf, dbo.tblDLtrans.sInvSrcCurrencyIDf, sCashTransType, sMatchTransSourceIDf, curInvSrcAmount, sInvSrcCurrencyIDf" + accSegSQLGroupBy +") as A\n" +
                            "                UNION \n" +
                            "                Select * \n" +
                            "                from(SELECT dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                            "                    ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                            "                    ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                            "                    ,(select sum(curAmount) from tblTEDocument where sDocNum = @paymentNumber and sPlayerNumIDf = @customerID and sTransSourceIDf = 'ARC') as pmtTotal2\n" +
                            "                    ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                            "                    ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                            "			 ,dbo.tbltetrans.sMatchSessionNumIDf as appliedToSessionID\n" +
                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                            "                    ,dbo.tblARCustomer.sName as customerName\n" +
                            "                    ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                            "                    ,dbo.tblTEtrans.sCurrencyIDf as currency\n" + accSegSQL + " \n" +
                            "                    ,dbo.tblTEtrans.sCashTransType as cashTransType\n" +
                            "                    ,dbo.tblTEtrans.sMatchTransSourceIDf as matchTransSource\n" +
                            "                    ,dbo.tblTEtrans.curInvSrcAmount as curInvSrcAmount\n" +
                            "	   		 ,dbo.tblTEtrans.sInvSrcCurrencyIDf as invSrcCurrency\n" +
                            "FROM [dbo].[tblteTrans]\n" +
                            "                INNER JOIN\n" +
                            "                dbo.tblteDocument\n" +
                            "                on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                            "                inner join dbo.tblARCustomer\n" +
                            "                on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                            "                where sOrigTransSourceIDf = 'ARC' and dbo.tbltetrans.[sOrigDocNum] = @paymentNumber and dbo.tblARCustomer.sCustomerID = @customerID " + appendedWhereClause + "\n" +
                            "                group by dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], sMatchSessionNumIDf, dbo.tblARCustomer.sCustomerID, dbo.tblARCustomer.sName, dbo.tblTEDocument.sPayMethod, dbo.tblTEtrans.sCurrencyIDf, dbo.tblTEtrans.sInvSrcCurrencyIDf, sCashTransType, sMatchTransSourceIDf, curInvSrcAmount, sInvSrcCurrencyIDf" + accSegSQLGroupBy +") as B) as C\n" +
                            "                group by pmtReferenceNo, pmtDate, appliedToRefNo, appliedToID, appliedToSessionID, customerNo, customerName, payMethod,currency, cashTransType, matchTransSource, invSrcCurrency" + accSegSQLGroupBy +
                            "                order by cashTransType asc\n";
            
            //Execute the Query
            abilaOriginalPaymentDetail = stmt.executeQuery( "DECLARE @customerID VARCHAR(30)\n" +
                                                            "DECLARE @paymentNumber VARCHAR(30)\n" +
                                                            "SET @customerID = '" + customerID + "'\n" +
                                                            "SET @paymentNumber = '" + paymentReferenceNo + "'\n" +
                                                            "\n" +
                                                            "Select ROW_NUMBER() OVER (order by pmtReferenceNo) as 'Order',  pmtReferenceNo, pmtDate, sum(cast(round(pmtAmount,2) as numeric(36,2)) * -1) as pmtLineAmount, sum(cast(round(pmtTotal2,2) as numeric(36,2))) as pmtTotal, appliedToRefNo, appliedToID, appliedToSessionID, customerNo, customerName, payMethod, currency, cashTransType, matchTransSource, sum(cast(round(curInvSrcAmount,2) as numeric(36,2))) as curInvSrcAmount, invSrcCurrency" + accSegSQLGroupBy + " from (Select *  from(SELECT dbo.tbldltrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbldltrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbldltrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,(select sum(curAmount) from tblDLDocument where sDocNum = @paymentNumber and sPlayerNumIDf = @customerID and sTransSourceIDf = 'ARC') as pmtTotal2\n" +  
                                                            "                    ,dbo.tbldltrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbldltrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "			 ,dbo.tbldltrans.sMatchSessionNumIDf as appliedToSessionID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblARCustomer.sName as customerName\n" +
                                                            "                    ,dbo.tblDLDocument.sPayMethod as payMethod\n" + 
                                                            "                    ,dbo.tblDLtrans.sCurrencyIDf as currency\n" + accSegSQL + " \n" +
                                                            "                    ,dbo.tblDLtrans.sCashTransType as cashTransType\n" +
                                                            "                    ,dbo.tblDLtrans.sMatchTransSourceIDf as matchTransSource\n" +
                                                            "                    ,dbo.tblDLtrans.curInvSrcAmount as curInvSrcAmount\n" +
                                                            "	   		 ,dbo.tblDLtrans.sInvSrcCurrencyIDf as invSrcCurrency\n" +
                                                            "FROM [dbo].[tblDLTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblDLDocument\n" +
                                                            "                on dbo.tblDLDocument.ctrDocID = dbo.tblDLTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblDLDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf = 'ARC' and dbo.tbldltrans.[sOrigDocNum] = @paymentNumber and dbo.tblARCustomer.sCustomerID = @customerID " + appendedWhereClause + "\n" +
                                                            "                group by dbo.tbldltrans.[sOrigDocNum], dbo.tbldltrans.[sMatchDocNum], dbo.tbldltrans.[dMatchDocIDf], sMatchSessionNumIDf, dbo.tblARCustomer.sCustomerID, dbo.tblARCustomer.sName, dbo.tblDLDocument.sPayMethod, dbo.tblDLtrans.sCurrencyIDf, dbo.tblDLtrans.sInvSrcCurrencyIDf, sCashTransType, sMatchTransSourceIDf, curInvSrcAmount, sInvSrcCurrencyIDf" + accSegSQLGroupBy +") as A\n" +
                                                            "                UNION \n" +
                                                            "                Select * \n" +
                                                            "                from(SELECT dbo.tbltetrans.[sOrigDocNum] as pmtReferenceNo\n" +
                                                            "                    ,max(dbo.tbltetrans.[dtmPostTo]) as pmtDate\n" +
                                                            "                    ,sum(cast(round(dbo.tbltetrans.[curAmount],2) as numeric(36,2)) * -1) as pmtAmount\n" +
                                                            "                    ,(select sum(curAmount) from tblTEDocument where sDocNum = @paymentNumber and sPlayerNumIDf = @customerID and sTransSourceIDf = 'ARC') as pmtTotal2\n" +
                                                            "                    ,dbo.tbltetrans.[sMatchDocNum] as appliedToRefNo\n" +
                                                            "                    ,dbo.tbltetrans.[dMatchDocIDf] as appliedToID\n" +
                                                            "			 ,dbo.tbltetrans.sMatchSessionNumIDf as appliedToSessionID\n" +
                                                            "                    ,dbo.tblARCustomer.sCustomerID as customerNo\n" +
                                                            "                    ,dbo.tblARCustomer.sName as customerName\n" +
                                                            "                    ,dbo.tblTEDocument.sPayMethod as payMethod\n" +
                                                            "                    ,dbo.tblTEtrans.sCurrencyIDf as currency\n" + accSegSQL + " \n" +
                                                            "                    ,dbo.tblTEtrans.sCashTransType as cashTransType\n" +
                                                            "                    ,dbo.tblTEtrans.sMatchTransSourceIDf as matchTransSource\n" +
                                                            "                    ,dbo.tblTEtrans.curInvSrcAmount as curInvSrcAmount\n" +
                                                            "	   		 ,dbo.tblTEtrans.sInvSrcCurrencyIDf as invSrcCurrency\n" +
                                                            "FROM [dbo].[tblteTrans]\n" +
                                                            "                INNER JOIN\n" +
                                                            "                dbo.tblteDocument\n" +
                                                            "                on dbo.tblteDocument.ctrDocID = dbo.tblteTrans.dDocID\n" +
                                                            "                inner join dbo.tblARCustomer\n" +
                                                            "                on dbo.tblteDocument.sPlayerNumIDf = dbo.tblARCustomer.sCustomerID\n" +
                                                            "                where sOrigTransSourceIDf = 'ARC' and dbo.tbltetrans.[sOrigDocNum] = @paymentNumber and dbo.tblARCustomer.sCustomerID = @customerID " + appendedWhereClause + "\n" +
                                                            "                group by dbo.tbltetrans.[sOrigDocNum], dbo.tbltetrans.[sMatchDocNum], dbo.tbltetrans.[dMatchDocIDf], sMatchSessionNumIDf, dbo.tblARCustomer.sCustomerID, dbo.tblARCustomer.sName, dbo.tblTEDocument.sPayMethod, dbo.tblTEtrans.sCurrencyIDf, dbo.tblTEtrans.sInvSrcCurrencyIDf, sCashTransType, sMatchTransSourceIDf, curInvSrcAmount, sInvSrcCurrencyIDf" + accSegSQLGroupBy +") as B) as C\n" +
                                                            "                group by pmtReferenceNo, pmtDate, appliedToRefNo, appliedToID, appliedToSessionID, customerNo, customerName, payMethod,currency, cashTransType, matchTransSource, invSrcCurrency" + accSegSQLGroupBy +
                                                            "                order by cashTransType asc\n");
            


            //Scroll through results
            while(abilaOriginalPaymentDetail.next() && !stop){
                
                
                
                //Get the payment amount recorded in Abila to determine if the Bill and Pay and Abila Payment are eual before adding the transactions.
                String origPmtAmount = abilaOriginalPaymentDetail.getString("pmtTotal");  
                origPmtAmountMod = (Double.valueOf(origPmtAmount));
                
                //Check if the retutn amount from Bill and Pay matches the amount of the payment in Abila. The return amount is a positive value in Bill and Pay.
                if(Objects.equals(origPmtAmountMod, Double.valueOf(paymentAmount))){
                    //Returned Payment amount from Bill and Pay is the same as the total calculated from the SQL statement. The return transaction can be registered in Abila.
                    
                    //Get remaining values from SQL query
                    String rowNo = abilaOriginalPaymentDetail.getString("Order");
                    origPmtDate = abilaOriginalPaymentDetail.getString("pmtDate");
                    String origLineAmount = abilaOriginalPaymentDetail.getString("pmtLineAmount");  
                    double origLineAmountMod = (Double.valueOf(origLineAmount) * -1);
                    String origDocRefNo = abilaOriginalPaymentDetail.getString("appliedToRefNo");
                    String origDocID = abilaOriginalPaymentDetail.getString("appliedToID");
                    String origSessionNo = abilaOriginalPaymentDetail.getString("appliedToSessionID");
                    origCustomerName = abilaOriginalPaymentDetail.getString("customerName");
                    String origCashTransType = abilaOriginalPaymentDetail.getString("cashTransType");
                    String origMatchTransSource = abilaOriginalPaymentDetail.getString("matchTransSource");
                    String origCurInvSrcAmount = abilaOriginalPaymentDetail.getString("curInvSrcAmount");
                    double origCurInvSrcAmountMod = (Double.valueOf(origCurInvSrcAmount) * -1);
                    String origInvSrcCurrency = abilaOriginalPaymentDetail.getString("InvSrcCurrency");
                
                    //Build string to include in SQL statement for current account distribution.
                    String origAccountDistribution = "";
                    int ctr = 0;
                    for(AccountSegment as: MainFrame.accountSegments){
                        String accSegTitle = "sCodeIDf_"+ MainFrame.accountSegments.get(ctr).getId();
                        origAccountDistribution += ",@" + accSegTitle + "=N'" + trim(abilaOriginalPaymentDetail.getString(accSegTitle)) + "'";
                        ctr++;
                    }
                    
                    //Create Abila Session if not already created.
                    if(!returnedPaymentsSessionCreated){
                        insertAbilaSession(buildAbilaInsertSession("ARC", "BPSYNC Returned Receipts"));
                        returnedPaymentsSessionCreated = true;
                    }
                    
                    //Create document record if it hasn't been created
                    if(rowNo.equals("1")){
                        String sqlDoc = "exec spTEDocInsert @ctrDocID=" + pmtID + ",@sSessionNumIDf=N'" +  ARCSessionID + "',@sTransSourceIDf=N'ARC',@sDocNum=N'" + paymentReferenceNo + "',@sDepositNum=N'',@sAddressee=N'" + origCustomerName + "',@sAddress=N'',@sDescription=N'" + pmtDesc + "',@sPlayerNumIDf=N'" + customerID + "',@sPlayerTypeIDf=N'C',@sAddressID=N'',@dtmDocDate='" + origPmtDate + "',@dtmDueDate='1899-12-28 00:00:00',@dtmPosted='1899-12-28 00:00:00',@curAmount=" + Double.toString(origPmtAmountMod * -1) + ",@curSrcAmount=" + Double.toString(origPmtAmountMod * -1) + ",@sCurrencyIDf=N'USD',@sRateType=N'',@ysnInvoiceAdjustment=0,@dDocLinkID=0,@sCreatedBy=N'ADMIN',@sCreatedAt=N'EBCO2',@dtmCreated='" + origPmtDate + "',@sModifiedBy=N'ADMIN',@sModifiedAt=N'EBCO2',@dtmModified='" + origPmtDate + "',@ysnLiquidateENC=0,@ysnEPay=0,@ysnPrepayment=0,@sBudgetVersion=N'',@s1099TypeID=N'',@sCreditType=N'',@sPayMethod=N'Bill and Pay',@sCCAType=N'',@sCCAHolder=N''";
                        if(abilaVersion.compareTo("17") > 0){
                            sqlDoc += ",@ysnAutoRevInv=0,@dtmAutoRevInv='1899-12-28 00:00:00'";
                        }
                        sqlStatements.add(sqlDoc);
                    }
                    

                    //Build transaction line SQL statement and add to the aray of atatements to be executed in ABila database.
                    String sqlLine = "exec spTEDocTELineInsert @sOrigDocNum=N'" + paymentReferenceNo + "',@sOrigSessionNumIDf=N'" +  ARCSessionID + "',@sOrigTransSourceIDf=N'ARC',@nOrderID=" + rowNo + ",@dtmPostTo='" + origPmtDate + "',@sEntryType=N'N',@sCashTransType=N'" + origCashTransType + "',@sDescription=N'" + pmtDesc + "',@s1099BoxNum=N'N/A',@dMatchDocIDf=" + origDocID + ",@sMatchDocNum=N'" + origDocRefNo + "',@sMatchSessionNumIDf=N'" + origSessionNo + "',@sMatchTransSourceIDf=N'" + origMatchTransSource + "',@ysnExchangeRateLocked=1,@nLineLinkID=0,@nEncLineLinkID=0,@dDocId=" + pmtID + origAccountDistribution + ",@curAmount=" + Double.toString(origLineAmountMod) + ",@curSrcAmount=" +  Double.toString(origLineAmountMod) + ",@decExchangeRate=1,@sCurrencyIDf=N'USD',@curInvSrcAmount=" +  Double.toString(origCurInvSrcAmountMod) + ",@sInvSrcCurrencyIDf=N'" + origInvSrcCurrency + "',@decInvSrcExchangeRate=1";
                    sqlStatements.add(sqlLine);
                    
                    
                }else{
                    //The Returned Payment amount from Bill and Pay does not match with the total calculated from the SQL statement. Update log and progress monitor.
                    
                    //Log information.
                    intermediateJTextArea.append("Return for Payment: "+ paymentReferenceNo + " of $" + paymentAmount + " for Customer: " + customerID + " cannot be returned because it doesn't match the amount registered, the returned payment is already registered, or was not found in Abila.\n");
                    log.writeToFile("Return for Payment: "+ paymentReferenceNo + " of $" + paymentAmount + " for Customer: " + customerID + " cannot be returned because it doesn't match the amount registeredthe returned payment is already registered, or was not found in Abila.\n");
                    
                    //Flag transaction to not continue while loop.
                    stop = true;
                }
                
                
            }
            
            if(!stop){
                    //Execute queries
                    for(String sql: sqlStatements){
                        executeAbilaSQLStatement(sql);
                    }
                    
                    //Log information.
                    intermediateJTextArea.append("Succesfully added Returned Payment: " + paymentReferenceNo + " of $" + paymentAmount + " for Customer: " + customerID + "\n");
                    log.writeToFile("Succesfully added Returned Payment: " + paymentReferenceNo + " of $" + paymentAmount + " for Customer: " + customerID + "\n");
                }
            
         }catch (Exception e){
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, e);
        }
    }
    
    public void getAbilaVersion(){
        intermediateJTextArea.append("Getting Abila version...\n");
        log.writeToFile("Getting Abila version...\n");
        getSQLProperties();
        
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, PASSWORD);
            Statement stmt = connection.createStatement();          
            ResultSet accountingSegmentsResultSet;
            
            accountingSegmentsResultSet = stmt.executeQuery("SELECT [sVersion]\n" +
                                                            "  FROM [NpsSqlSys].[dbo].[tblSysInfo]");
                
        
            
            while(accountingSegmentsResultSet.next()){
                abilaVersion = accountingSegmentsResultSet.getString("sVersion");
            }
            
        }catch (Exception e){
            e.printStackTrace();
            intermediateJTextArea.append(e.toString() + "\n");
            log.writeToFile(e.toString() + "\n");
            this.cancel(true);
            JOptionPane.showMessageDialog(null, e);
        }
        
        intermediateJTextArea.append("Abila version is " + abilaVersion + "...\n");
        log.writeToFile("Abila version is " + abilaVersion + "...\n");
    }
}
