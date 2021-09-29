/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package abilabpsync;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.httpclient.NameValuePair;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author LuisA
 */
public class PerformPostCallCustomerAdd {
    
    String line = "";
    
    private String getQuery(List<NameValuePair> params) throws UnsupportedEncodingException{
        StringBuilder result = new StringBuilder();
        boolean first = true;

        for (NameValuePair pair : params)
        {
            if (first)
                first = false;
            else
                result.append("&");

            result.append(URLEncoder.encode(pair.getName(), "UTF-8"));
            result.append("=");
            result.append(URLEncoder.encode(pair.getValue(), "UTF-8"));
        }

        return result.toString();
    }
   
    public void performPostCall() throws MalformedURLException, IOException, SAXException, ParserConfigurationException{
        URL url = new URL("https://www.billandpay.com/webservices/service.php");
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setRequestProperty("User-Agent", "Mozilla/5.0"); 
        conn.setReadTimeout(10000);
        conn.setConnectTimeout(15000);
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new NameValuePair("xml", "<?xml version=\"1.0\"?> <request> <response> <type>xml</type> </response> <biller> <authenticate> <id>"+MainFrame.billerID+"</id> <password>"+MainFrame.billerPassword+"</password> </authenticate> <customerinfo> </customerinfo> </biller> </request>"));

        OutputStream os = conn.getOutputStream();
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8"));
        writer.write(getQuery(params));
        writer.flush();
        writer.close();
        os.close();

        conn.connect();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        //line = bufferedReader.read();
        String lineB;
            while ((lineB = bufferedReader.readLine()) != null) {
              //System.out.print(lineB);
              lineB += lineB;
              //System.out.println(lineB);
            }
            System.out.println("\n"+lineB);
        xmlParse parse = new xmlParse(stringToDocument(lineB));
        parse.setDom(stringToDocument(lineB)); 
        System.out.println("after instantiating parse");   
    }
    
    public void performPostCallAddCustomer(String xml) throws MalformedURLException, IOException, SAXException, ParserConfigurationException{
        
        URL url = new URL("https://www.billandpay.com/webservices/service.php");
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setRequestProperty("User-Agent", "Mozilla/5.0"); 
        conn.setReadTimeout(10000);
        conn.setConnectTimeout(15000);
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new NameValuePair("xml", xml));

        OutputStream os = conn.getOutputStream();
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(os, "UTF-8"));
        writer.write(getQuery(params));
        writer.flush();
        writer.close();
        os.close();

        conn.connect();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream())); //This is when the customer gets added.
        //line = bufferedReader.read();
        StringBuilder builder = new StringBuilder();
        line = "";
        
        while ((line = bufferedReader.readLine()) != null) {
            builder.append(line);
        }

        String text = builder.toString();
        System.out.println("\n"+text);
        
        /**String tempLine;
        while ((tempLine = bufferedReader.readLine()) != null) {
          //System.out.print(lineB);
          line += tempLine;
          //System.out.println(lineB);
        }**/
        
        //System.out.println("\n"+line);
        xmlParseAddCustomer parse = new xmlParseAddCustomer(stringToDocument(text));
        //parse.setDom(stringToDocument(line)); 
        //System.out.println("after instantiating parse"); 
    }
    
    public Document stringToDocument(String xmlSource)   
    throws SAXException, ParserConfigurationException, IOException {  
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
        DocumentBuilder builder = factory.newDocumentBuilder();  

        return builder.parse(new InputSource(new StringReader(xmlSource)));  
    } 
}
