/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package abilabpsync;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Luis A. Baez-Black
 */
public class EventLog {
    private String fileName;
    private PrintStream logFile;
    private PrintStream logOutputFS;
    private String dateTimeStamp;
    
    public EventLog() {
        try{
            fileName = new SimpleDateFormat("yyyyMMddhhmmss'.txt'").format(new Date());
            logFile = new PrintStream(new FileOutputStream(fileName));
        }
        catch(SecurityException securityException){}
        catch(FileNotFoundException fileNotFoundException){}
    }
    
    public void writeToFile(String str){
        dateTimeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date());
        logFile.print("[" + dateTimeStamp + "] " + str);
        logFile.println();
    }
    

    public void close(){
        logFile.close();
    }
    
    
}
