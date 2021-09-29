
package abilabpsync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
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
public class xmlParseUpdateCustomer {
    
    Document dom;
    //ArrayList<Customer> bpCustomers = new ArrayList<Customer>();

    public xmlParseUpdateCustomer(Document dom) {
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
        
    public String parseDocument(){
		//get the root element
		Element docEle = dom.getDocumentElement();
                String refNo = "";
                
                String custID = "";
                String CustName = "";
                String errorNum = "";
                String errorDesc = "";
                boolean isError = false;
                
                /**if( docEle.getElementsByTagName("error").getLength() != 0 )
                {
                    isError = true;
                    NodeList nl = docEle.getElementsByTagName("error");
                    if(nl != null && nl.getLength() > 0) {
			for(int i = 0 ; i < nl.getLength();i++) {

				//get the employee element
				Element el = (Element)nl.item(i);
                                errorNum = getTextValue(el, "number");
                                errorDesc = getTextValue(el, "description");
                                

                                System.out.println("Couldn't add client\n" + "Error number: " + errorNum + "\nError Description: " + errorDesc + "\n");
			}
                    }
                }**/
                
                isError = docEle.getElementsByTagName("error").getLength() != 0;

		
                custID = docEle.getElementsByTagName("id").item(0).getFirstChild().getNodeValue();
		//CustName = docEle.getElementsByTagName("name").item(0).getFirstChild().getNodeValue();
                //get a nodelist of elements
                /**NodeList nl = docEle.getElementsByTagName("customeradd");
                
                
		if(nl != null && nl.getLength() > 0) {
			for(int i = 0 ; i < nl.getLength();i++) {

				//get the employee element
				Element el = (Element)nl.item(i);
                                custID = getTextValue(el, "id");
                                errorNum = getTextValue(el, "number");
                                errorDesc = getTextValue(el, "description");

                                
			}
		}**/
                
                if(isError){
                    errorNum = docEle.getElementsByTagName("number").item(0).getFirstChild().getNodeValue();
                    errorDesc = docEle.getElementsByTagName("description").item(0).getFirstChild().getNodeValue();
                    MainFrame.progressMonitor.append("Couldn't update Client ID: " + custID + "\nError number: " + errorNum + "\nError Description: " + errorDesc + "\n");
                    System.out.println("Couldn't add Client ID: " + custID + "\nError number: " + errorNum + "\nError Description: " + errorDesc + "\n");
                    BackgroundSync.log.writeToFile("Couldn't update Client ID: " + custID + "\nError number: " + errorNum + "\nError Description: " + errorDesc + "\n");
                    //System.out.println("Couldn't update Client ID: " + custID + "\nError number: " + errorNum + "\nError Description: " + errorDesc + "\n");
                }else{
                    BackgroundSync.log.writeToFile("Successfully updated Client ID: " + custID + "...\n");
                }
                
                return custID;
	}
    
    
    
    private Customer getCustomer(Element custEl) {
		
		//for each <employee> element get text or int values of 
		//name ,id, age and name
		String id = getTextValue(custEl,"id");
                String companyName = getTextValue(custEl,"companyname");
                String firstName = getTextValue(custEl,"firstname");
		String middleInitial = getTextValue(custEl,"middlename");
                String lastName = getTextValue(custEl,"lastname");
		String status = custEl.getAttribute("active");
                String email = getTextValue(custEl,"email");

		
		
		//Create a new Employee with the value read from the xml nodes
		Customer c = new Customer(id, companyName, firstName, middleInitial, lastName, status, email);
		
		return c;
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
