
package abilabpsync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author LuisA
 */
public class xmlParsePayments {
    
    Document dom;

    public xmlParsePayments(Document dom) {
        this.dom = dom;
        this.parseDocument();
        //this.printData();
    }
    
    
    
    private void parseXmlFile(String xml){
        //get the factory
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

        try {

                //Using factory get an instance of document builder
                DocumentBuilder db = dbf.newDocumentBuilder();

                //parse using builder to get DOM representation of the XML file
                dom = db.parse(xml);


        }catch(ParserConfigurationException pce) {
                pce.printStackTrace();
        }catch(SAXException se) {
                se.printStackTrace();
        }catch(IOException ioe) {
                ioe.printStackTrace();
        }
    }
        
    private void parseDocument(){		
                MainFrame.progressMonitor.append("Reading Payments from Bill and Pay...\n");		
                
                Element docEle = dom.getDocumentElement();
                NodeList pnl = docEle.getElementsByTagName("payment");
                NodeList cnl = docEle.getElementsByTagName("customer");
                NodeList inl = docEle.getElementsByTagName("appliedto");
                //String cn = "";
                
                int pnlLength = pnl.getLength();
                int cnlLength = cnl.getLength();
                int inlLength = inl.getLength();
                
                
		if(pnl != null && pnl.getLength() > 0) {
                    for(int i = 0 ; i < pnl.getLength();i++) {
                        
                        NodeList inlb = ((Element)pnl.item(i)).getElementsByTagName("invoice");
                        int invoiceSize = inlb.getLength();
                        //String unnapliedBalance = getTextValue((Element) pnl.item(i),"unappliedbalance");
                        ArrayList<Invoice> invoices = new ArrayList<Invoice>();
                        ArrayList<String> appliedAmt = new ArrayList<String>();
                        
                        if(invoiceSize > 0){ //main if starts here
                            
                            for(int j = 0; j < invoiceSize; j++){
                                Invoice inv = getInvoice((Element)inlb.item(j));
                                invoices.add(inv);
                                appliedAmt.add(getAppliedAmount((Element)inlb.item(j)));
                            }
                        }

                            //get the Payment and Customer elements
                            Element pmtEl = (Element)pnl.item(i);
                            Element cusEl = (Element)cnl.item(i);
                            

                            //get the Payment
                            Payment pmt = getPayment(pmtEl, cusEl, invoices, appliedAmt);
                            
                            
                            //add payment to list
                            MainFrame.bpPayments.add(pmt);
                            
                        //}///main if ends here
                    }
		}
                    
                MainFrame.progressMonitor.append("A total of " + MainFrame.bpPayments.size() + " Payments read from Bill and Pay...\n");
	}
    
    private Payment getPayment(Element pmtEl, Element custEl, ArrayList<Invoice> invoices, ArrayList<String> appliedAmt){
        String pmtMethod = "";

        //Customer element	
        String custID = getTextValue(custEl,"id");
        String custName = getTextValue(custEl,"companyname");
        
        //Payment element
        String pmtAmount = getTextValue(pmtEl,"amount");
        String pmtDate = getTextValue(pmtEl,"date");        
        String pmtRef = getTextValue(pmtEl,"referencenumber");
         String pmtMethod2 = getTextValue(pmtEl,"method");
        if(getTextValue(pmtEl,"method") != null && !getTextValue(pmtEl,"method").isEmpty() && !pmtMethod2.equals("Cash") && !pmtMethod2.equals("Check")){
            pmtMethod = getTextValue(pmtEl,"method");
        }else{            
            pmtMethod = "BP Cash-Check";
        }
        String lastUpdatedDate = getTextValue(pmtEl,"updateddatetime");

        //Create a new Payment with the value read from the xml nodes
        Payment pmt = new Payment(pmtDate, pmtMethod, pmtAmount, custID, custName, pmtRef, invoices, appliedAmt, lastUpdatedDate);

        return pmt;
    }
    
    private Invoice getInvoice(Element InvEl) {
	      
        //Invoice element
        String invId = getTextValue(InvEl,"id");
        String invNo = getTextValue(InvEl,"number");
        

        //Create a new Invoice with the value read from the xml nodes
        Invoice inv = new Invoice(invId, invNo);

        return inv;
	}
    
    private String getAppliedAmount(Element amtEl){
        return getTextValue(amtEl,"amount");        
    }
 
    private String getTextValue(Element ele, String tagName) {
		String textVal = null;
		NodeList nl = ele.getElementsByTagName(tagName);
		if(nl != null && nl.getLength() > 0) {
			Element el = (Element)nl.item(0);
			textVal = el.getFirstChild().getNodeValue();
		}

		return textVal;
	}
    
    private void printData(){

		/**System.out.println("No of Customers '" + MainFrame.bpCustomers.size() + "'.");

		Iterator it = MainFrame.bpCustomers.iterator();
		while(it.hasNext()) {
			System.out.println(it.next().toString());
		}**/
	}

    public void setDom(Document dom) {
        this.dom = dom;
    }
    
    
}
