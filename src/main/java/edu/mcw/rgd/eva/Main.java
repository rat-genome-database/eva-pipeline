package edu.mcw.rgd.eva;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import java.util.*;

public class Main {
    private String version;
    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--importEva":
                        EvaImport evaImport = (EvaImport) (bf.getBean("evaImport"));
                        evaImport.run();
                        return;
                    case "--exportEva":
                        return;
                }
            }
        }
        catch (Exception e) {
            Utils.printStackTrace(e, LogManager.getLogger("status"));
            throw e;
        }
    }

    public void run2() throws Exception { // Using the EVA API
        XmlBeanFactory bf = new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        EvaApiDownloader temp = (EvaApiDownloader)(bf.getBean("evaApiDownloader"));
    }

    public void runAPI(int mapKey, String chrom, List<VcfLine> data) throws Exception {
        XmlBeanFactory bf = new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        EvaApiDownloader temp = (EvaApiDownloader) (bf.getBean("evaApiDownloader"));
        temp.downloadWithAPI(mapKey,chrom,data);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}