/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package abilabpsync;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.ParserConfigurationException;
//import sun.net.www.http.HttpClient;
import org.apache.commons.httpclient.*;
import org.apache.http.client.methods.HttpPost;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.xml.sax.SAXException;


/**
 *
 * @author LuisA
 */
public class AbilaBPSync {

    /**
     * @param args the command line arguments
     */
    public static MainFrame mainFrame;
    
    public static void main(String[] args) throws MalformedURLException, IOException, SAXException, ParserConfigurationException {
    
        mainFrame = new MainFrame();
        mainFrame.setVisible(true);
      
    }
    
    
}
