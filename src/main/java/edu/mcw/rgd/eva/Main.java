package edu.mcw.rgd.eva;

import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.beans.factory.xml.XmlBeanFactory;
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

    private int totalInserted = 0, totalDeleted = 0;

    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.eva.Main mainBean = (edu.mcw.rgd.eva.Main) (bf.getBean("main"));
        try {
//            mainBean.run2();
            mainBean.run();
        } catch (Exception e) {
            Utils.printStackTrace(e, mainBean.logger);
            //e.printStackTrace();
            throw e;
        }
    } // end of main

    public void run() throws Exception{ // Gets VCF files from EVA
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        Set<Integer> mapKeys = getIncomingFiles().keySet();

        for (Integer mapKey : mapKeys) {
            long timeStart = System.currentTimeMillis();
            edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
            String assemblyName = assembly.getName();
            logger.info("   Assembly "+assemblyName+" started at "+sdt.format(new Date(timeStart)));
            String localFile = downloadEvaVcfFile(getIncomingFiles().get(mapKey), mapKey);
            extractData(localFile, mapKey);
            logger.info("   Finished updating database for assembly "+assemblyName);
            logger.info("   Total Eva objects removed:  "+totalDeleted);
            logger.info("   Total Eva objcets inserted: "+totalInserted);
            logger.info("   Eva Assembly "+assemblyName+" -- elapsed time: "+
                    Utils.formatElapsedTime(timeStart,System.currentTimeMillis())+"\n");
            totalDeleted = 0;
            totalInserted = 0;
        }
        logger.info("   Total Eva pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @param mapKey      - the map key to the assembly
     *****************************/
    public void extractData(String fileName, int mapKey) throws Exception {
        String[] col = null;
        logger.debug("  Extracting data from downloaded assembly file ");
        BufferedReader br = Utils.openReader(fileName);
        String lineData; // collects the data from the file lines
        int i = 0;
        int totalObjects = 0;
        ArrayList<VcfLine> VCFdata = new ArrayList<>();
        while ((lineData = br.readLine()) != null) {
            if (lineData.startsWith("#")) {
                if (lineData.charAt(1) != '#') {
                    col = lineData.split("\t"); // splitting the columns into an array for storage
                    col[0] = col[0].substring(1, col[0].length()); // removing the '#' in the first string
                }
                continue;
            }
            VCFdata.add(new VcfLine(lineData, col, mapKey)); // adds the line to the array list
            // go until chromosome changes
            if( VCFdata.size()>1 && !VCFdata.get(i).getChrom().equals(VCFdata.get(i-1).getChrom()) ) {
                // update db with all but last (VCFdata.subList(0,i))
                List<VcfLine> VCFbyChrom = VCFdata.subList(0,i);
                updateDB(VCFbyChrom, mapKey, VCFdata.get(i-1).getChrom());
//                runAPI(mapKey, VCFdata.get(i-1).getChrom(), VCFbyChrom);
                // clear list, then re-add current line data
                VCFdata.clear();
                totalObjects = totalObjects+i;
                VCFdata.add(new VcfLine(lineData, col, mapKey));
                i=0;
            }
            i++;
        } // end while
        List<VcfLine> VCFbyChrom = VCFdata.subList(0,i);
        updateDB(VCFbyChrom, mapKey, VCFdata.get(i-1).getChrom());
        totalObjects = totalObjects+i;
        logger.info("   Total Eva objects checked: "+totalObjects);
        br.close();
    }

    /*****************************
     * updateDB - converts the VCFdata to Eva objects then does set operations
     * @param VCFdata - the data from the VCF file
     * @param chromosome - current chromosome
     * @throws Exception
     *****************************/
    public void updateDB(List<VcfLine> VCFdata, int mapKey, String chromosome) throws Exception {
        ArrayList<Eva> incomingData = new ArrayList<>();
        dao.convertToEva(incomingData, VCFdata);
        dao.CalcPadBase(incomingData);
        insertAndDeleteEvaObjectsByKeyAndChromosome(incomingData, mapKey, chromosome);
    }

    /*****************************
     * insertAndDeleteEvaObjectsByKeyAndChromosome - compares the data in the database with the new data
     * @param incoming - incoming data to be compared
     * @param chromosome - current chromosome
     * @throws Exception
     *****************************/
    public void insertAndDeleteEvaObjectsByKeyAndChromosome(ArrayList<Eva> incoming, int mapKey, String chromosome) throws Exception {
        List<Eva> inRGD = dao.getEvaObjectsFromMapKeyAndChromosome(mapKey,chromosome);
        logger.debug("  Inserting and deleting Eva Objects");
        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        if (!tobeInserted.isEmpty()) {
            logger.info("   New Eva objects to be inserted in chromosome "+chromosome+": " + tobeInserted.size());
            totalInserted += tobeInserted.size();
            dao.insertEva(tobeInserted);
            tobeInserted.clear();
        }

        // determines old objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);
        if (!tobeDeleted.isEmpty()) {
            logger.info("   Old Eva objects to be deleted in chromosome "+chromosome+": " + tobeDeleted.size());
            totalDeleted+=tobeDeleted.size();
            dao.deleteEvaBatch(tobeDeleted);
            tobeDeleted.clear();
        }

        Collection<Eva> matching = CollectionUtils.intersection(inRGD, incoming);
        int matchingEVA = matching.size();
        if (matchingEVA != 0) {
            logger.info("   Eva objects that are matching in chromosome "+chromosome+": " + matchingEVA);
            matching.clear();
        }
    }
    String downloadEvaVcfFile(String file, int key) throws Exception{
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/EVA_"+key+".vcf");
        downloader.setUseCompression(true);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
    }

    public void run2() throws Exception { // Using the EVA API
        XmlBeanFactory bf = new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        EvaApiDownloader temp = (EvaApiDownloader)(bf.getBean("evaApiDownloader"));
        temp.downloadAllFiles(getVersion());

    }

    public void run3() throws  Exception {

        logger.info(getVersion());
        logger.info("   "+dao.getConnection());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        Set<Integer> mapKeys = getIncomingFiles().keySet();

        for (Integer mapKey : mapKeys) {
            long timeStart = System.currentTimeMillis();
            edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
            String assemblyName = assembly.getName();
            logger.info("   Assembly "+assemblyName+" started at "+sdt.format(new Date(timeStart)));
            String localFile = downloadEvaVcfFile(getIncomingFiles().get(mapKey), mapKey);
            extractData(localFile, mapKey);
            logger.info("   Finished updating database for assembly "+assemblyName);
            logger.info("   Eva Assembly "+assemblyName+" -- elapsed time: "+
                    Utils.formatElapsedTime(timeStart,System.currentTimeMillis())+"\n");
        }
        logger.info("   Total Eva pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));

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

    public void setIncomingFiles(Map<Integer, String> incomingFiles) {
        this.incomingFiles = incomingFiles;
    }

    public Map<Integer, String> getIncomingFiles() {
        return incomingFiles;
    }
}