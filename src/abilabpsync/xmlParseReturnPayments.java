
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
public class xmlParseReturnPayments {
    
    Document dom;

    public xmlParseReturnPayments(Document dom) {
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
                MainFrame.progressMonitor.append("Reading Returned Payments from Bill and Pay...\n");		
                
                Element docEle = dom.getDocumentElement();
                NodeList tnl = docEle.getElementsByTagName("transaction");
                NodeList rnl = docEle.getElementsByTagName("return");
                
		if(tnl != null && tnl.getLength() > 0) {
                    for(int i = 0 ; i < tnl.getLength();i++) {
                        //get the Return and Transaction elements
                        Element tranEl = (Element)tnl.item(i);
                        Element retEl = (Element)rnl.item(i);
                        //get the Payment
                        Payment pmt = getReturnedPayment(tranEl, retEl);
                        //add payment to list
                        MainFrame.returnedPayments.add(pmt);
                    }
                }  
                MainFrame.progressMonitor.append("A total of " + MainFrame.returnedPayments.size() + " Returned Payments read from Bill and Pay...\n");
	}
    
    private Payment getReturnedPayment(Element tEl, Element rEl){
        //Custoemr element	
        String paymentNumber = getTextValue(tEl,"internalid");
        String customerID = getTextValue(tEl,"id");
        String paymentAmt = getTextValue(tEl,"amount");
        String lastUpdatedDate = getTextValue(rEl,"returndatetime");
        

        //Create a new Payment with the value read from the xml nodes
        Payment pmt = new Payment(paymentNumber, customerID, paymentAmt, lastUpdatedDate, lastUpdatedDate);

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
