package com.ictrui.datahub_lineage.Entity;

import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.NoSuchFileException;

@Component
@Slf4j
public class Pythonemitter {

//    @Value("${com.eirunye.defproname}")
//    public  static String PYTHON_HOME = "";

    @Value("${json.pythonfile}")
    private String EXECUTE_PATH;

    @Value("${datahubgms.url}")
    private String datahubgms_url;


    public void check(){
        int status = 404;
        try {
            URL urlObj = new URL(datahubgms_url);
            HttpURLConnection oc = (HttpURLConnection) urlObj.openConnection();
            oc.setUseCaches(false);
            oc.setConnectTimeout(3000); // 设置超时时间
            status = oc.getResponseCode();// 请求状态
        } catch (Exception e) {
            throw new RuntimeException(new ConnectException("the datahub's url is not avaiable"));
        }
        File file = new File(EXECUTE_PATH);
        if (!file.exists()) {
            throw new RuntimeException(new NoSuchFileException("The python file not exits"));
        }

//        log.info("check the path of file is right or the file is exits");
    }

    public void run(Json json)  {
        log.info("EXECUTE_PATH is {}",EXECUTE_PATH);
        log.info("The datahubgms_url is {}",datahubgms_url);
        log.info("Current PATH is {}",System.getProperty("user.dir"));

        String[] args = new String[] { "python",EXECUTE_PATH, json.getReadername(), json.getReadertable().toString(),
                json.getWritername(), json.getWritertable().toString(),datahubgms_url};
        try {
            Process proc = Runtime.getRuntime().exec(args);     //execute the
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
                log.info("the python output is {}",line);
            }
            in.close();
            proc.waitFor();
            log.info("The python script has benn finished");
        } catch (IOException e) {
            log.error("The python script has been shutdowning because of IOException");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.error("The python script has been shutdowning because of InterruptedException");
            throw new RuntimeException(e);
        }
    }
}
