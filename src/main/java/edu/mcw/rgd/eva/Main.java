package edu.mcw.rgd.eva;

import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private String version;
    private Map<Integer, String> incomingFiles;

    protected Logger logger = Logger.getLogger("status");

    private DAO dao = new DAO();

    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.eva.Main mainBean = (edu.mcw.rgd.eva.Main) (bf.getBean("main"));
        try {
            mainBean.run();
        } catch (Exception e) {
            Utils.printStackTrace(e, mainBean.logger);
            throw e;
        }
    } // end of main

    public void run() throws Exception{
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        Set<Integer> mapKeys = getIncomingFiles().keySet();
        File directory = new File("data/");
        if (!directory.exists())
            directory.mkdir();
        for (Integer mapKey : mapKeys) {
            long timeStart = System.currentTimeMillis();
            edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
            String assemblyName = assembly.getName();
            logger.info("   Assembly "+assemblyName+" started at "+sdt.format(new Date(timeStart)));
            ArrayList<VcfLine> VCFdata = new ArrayList<>();
            String localFile = downloadEvaVcfFile(getIncomingFiles().get(mapKey), mapKey);
            extractData(localFile, VCFdata, mapKey);
            updateDB(VCFdata, mapKey);
            logger.info("   Finished updating database for assembly "+assemblyName);
            logger.info("   Eva Assembly "+assemblyName+" -- elapsed time: "+
                    Utils.formatElapsedTime(timeStart,System.currentTimeMillis())+"\n");
        }
        logger.info("   Total Eva pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @param VCFdata  - the list that will be populated with incoming data
     * @param key      - the map key to the assembly
     *****************************/
    public void extractData(String fileName, ArrayList<VcfLine> VCFdata, int key) {
        String[] col = null;
        logger.debug("  Extracting data from downloaded assembly file ");
        try {
            BufferedReader br = Utils.openReader(fileName);
            String lineData; // collects the data from the file lines
            while ((lineData = br.readLine()) != null) {
                if (lineData.startsWith("#")) {
                    if (lineData.charAt(1) != '#') {
                        col = lineData.split("\t"); // splitting the columns into an array for storage
                        col[0] = col[0].substring(1, col[0].length()); // removing the '#' in the first string
                    }
                    continue;
                }
                VCFdata.add(new VcfLine(lineData, col, key)); // adds the line to the array list
            } // end while
            br.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    /*****************************
     * updateDB - converts the VCFdata to Eva objects then does set operations
     * @param VCFdata - the data from the VCF file
     * @throws Exception
     *****************************/
    public void updateDB(ArrayList<VcfLine> VCFdata, int key) throws Exception {
        ArrayList<Eva> incomingData = new ArrayList<>();
        dao.convertToEva(incomingData, VCFdata);
        insertAndDeleteEvaObjectsbyKey(incomingData, key);
    }

    /*****************************
     * insertAndRemoveEvaObjects - compares the data in the database with the new data
     * @param incoming - incoming data to be compared
     * @throws Exception
     *****************************/
    public void insertAndDeleteEvaObjectsbyKey(ArrayList<Eva> incoming, int key) throws Exception {
        List<Eva> inRGD = dao.getEvaObjectsFromMapKey(key);
        logger.debug("  Inserting and deleting Eva Objects");
        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        // determines old objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);

        Collection<Eva> matching = CollectionUtils.intersection(inRGD, incoming);

        if (!tobeInserted.isEmpty()) {
            dao.insertEva(tobeInserted);
            logger.info("   New Eva objects to be inserted: " + tobeInserted.size());
        }
        if (!tobeDeleted.isEmpty()) {
            dao.deleteEvaBatch(tobeDeleted);
            logger.info("   Old Eva objects to be deleted: " + tobeDeleted.size());
        }
        int matchingEVA = matching.size();
        if (matchingEVA != 0)
            logger.info("   Eva objects that are matching: " + matchingEVA);
    }

    String downloadEvaVcfFile(String file, int key) throws Exception{
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/EVA_"+key+".vcf");
        downloader.setUseCompression(true);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setIncomingFiles(Map<Integer, String> incomingFiles) {
        this.incomingFiles = incomingFiles;
    }

    public Map<Integer, String> getIncomingFiles() {
        return incomingFiles;
    }
}