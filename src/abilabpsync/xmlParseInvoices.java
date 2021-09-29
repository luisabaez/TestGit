
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
public class xmlParseInvoices {
    
    Document dom;

    public xmlParseInvoices(Document dom) {
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
                //MainFrame.progressMonitor.append("Reading Invoices from Bill and Pay...\n");		
                //BackgroundSync.intermediateJTextArea.append("Message from XMLParseInvoice...\n");
                Element docEle = dom.getDocumentElement();
                NodeList cnl = docEle.getElementsByTagName("customer");
                NodeList inl = docEle.getElementsByTagName("invoice");
                
		if(inl != null && inl.getLength() > 0) {
			for(int i = 0 ; i < inl.getLength();i++) {

				//get the invoice and customer
				Element invEl = (Element)inl.item(i);
                                Element cusEl = (Element)cnl.item(i);
                                
				//get the Invoice
				Invoice inv = getInvoice(invEl, cusEl);

				//add invoice to list
                                //if(inv.getCustomerID().equals("810") || inv.getCustomerID().equals("LABB") || inv.getCustomerID().equals("LABB2") || inv.getCustomerID().equals("LABB3")){
                                    //MainFrame.progressMonitor.append("Adding BP Invoice " + inv.getReferenceNumber() + "\n");
                                    MainFrame.bpInvoices.add(inv);
                                //}
				
			}
                    
		}
                    
                MainFrame.progressMonitor.append("A total of " + MainFrame.bpInvoices.size() + " Invoices read from Bill and Pay...\n");
                BackgroundSync.log.writeToFile("A total of " + MainFrame.bpInvoices.size() + " Invoices read from Bill and Pay...\n");
	}
    private Invoice getInvoice(Element InvEl, Element custEl) {
	//Custoemr element	
        String custID = getTextValue(custEl,"id");
        
        //Invoice element
        String invId = getTextValue(InvEl,"id");
        String invNo = getTextValue(InvEl,"number");
        String total = getTextValue(InvEl,"total");

        //Create a new Invoice with the value read from the xml nodes
        Invoice inv = new Invoice(invId, invNo, custID, total);

        return inv;
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
