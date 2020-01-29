package edu.mcw.rgd.eva;

import edu.mcw.rgd.process.Utils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Main {

    private String version;
    private Map<Integer, String> incomingFiles;

    protected Logger logger = Logger.getLogger("status");

    DAO dao = new DAO();

    public static void main(String[] args) {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.eva.Main mainBean = (edu.mcw.rgd.eva.Main) (bf.getBean("main"));
        try {
            mainBean.run();
        } catch (Exception e) { e.printStackTrace(); }
    } // end of main

    public void run() {
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info("   Pipeline started at "+sdt.format(new Date(pipeStart)));
        Set<Integer> mapKeys = getIncomingFiles().keySet();
        File directory = new File("data/");
        if (!directory.exists())
            directory.mkdir();
        try {
            for (Integer mapKey : mapKeys) {
                long timeStart = System.currentTimeMillis();
                logger.info("   Assembly started at "+sdt.format(new Date(timeStart)));
                ArrayList<VcfLine> VCFdata = new ArrayList<>();
                String[] urlcuts;
                String url, filename, assembly, VCFlinedata = "data/";
                url = getIncomingFiles().get(mapKey);
                urlcuts = url.split("/");
                filename = urlcuts[urlcuts.length - 1];
                assembly = urlcuts[urlcuts.length - 2];
                VCFlinedata += filename.substring(0, filename.length() - 3); // removes the .gz
                downloadUsingNIO(url, "data/" + filename);
                decompressGzipFile("data/" + filename, VCFlinedata);
                extractData(VCFlinedata, VCFdata, assembly, mapKey);
                updateDB(VCFdata, mapKey);
                logger.info("   Finished updating database for assembly "+assembly);
                logger.info("   Eva Assembly "+assembly+" -- elapsed time: "+
                        Utils.formatElapsedTime(timeStart,System.currentTimeMillis()));
            }
        } catch (Exception e) { e.printStackTrace(); }
        logger.info("   Total Eva pipeline runtime -- elapsed time: "+Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @param VCFdata  - the list that will be populated with incoming data
     * @param assembly - map name corresponding to the data
     * @param key      - the map key to the rnor
     *****************************/
    public void extractData(String fileName, ArrayList<VcfLine> VCFdata, String assembly, int key) {
        String[] col = null;
        logger.debug("  Extracting data from downloaded assembly file "+assembly);
        try {
            File f = new File(fileName); // placeholder for the file we are looking for
            String absolute = f.getAbsolutePath(); // gets the file path to file f
            File VCFfile = new File(absolute);
            BufferedReader br = new BufferedReader(new FileReader(VCFfile));
            String lineData; // collects the data from the file lines
            while ((lineData = br.readLine()) != null) {
                if (lineData.charAt(0) == '#') {
                    if (lineData.charAt(1) != '#') {
                        col = lineData.split("\t"); // splitting the columns into an array for storage
                        col[0] = col[0].substring(1, col[0].length()); // removing the '#' in the first string
                    }
                    continue;
                }
                VCFdata.add(new VcfLine(lineData, col, key)); // adds the line to the array list
            } // end while
            br.close();
            createFile(col, VCFdata, assembly, key);
        } catch (Exception e) { e.printStackTrace(); }
    }

    /*****************************
     * createFile - creates the tab separated file or not depending if it exists then calls writeTofile
     * @param col      - the column names
     * @param data     - the data from the VCF file
     * @param assembly - map name corresponding to the data
     * @throws Exception
     *****************************/
    public void createFile(String[] col, ArrayList<VcfLine> data, String assembly, int key) throws Exception {
        String tsvFile = "data/newEVAData-" + assembly + ".tsv";
        File dataFile = new File(tsvFile);
        if (dataFile.createNewFile()) // the only difference is whether the file is created or not
            writeTofile(dataFile, col, data, key);
        else
            writeTofile(dataFile, col, data, key);
    }

    /*****************************
     * writeTofile - writes the corresponding data into a tab separated file
     * @param dataFile - the name of the file in the directory
     * @param col      - the column names
     * @param data     - the data from the VCF file
     *****************************/
    public void writeTofile(File dataFile, String[] col, ArrayList<VcfLine> data, int key) {
        try {
            String absolute = dataFile.getAbsolutePath(); // gets the file path to file dataFile
            File TSVfile = new File(absolute); // finds and opens the dataFile into a tab file
            BufferedWriter writer = new BufferedWriter(new FileWriter(TSVfile));
            for (int i = 0; i < col.length; i++) { // adding the columns to the .tsv file
                if (col[i].toLowerCase().equals("qual") || col[i].toLowerCase().equals("filter"))
                    continue;
                if (i == col.length - 1)
                    writer.write(col[i]);
                else
                    writer.write(col[i] + "\t");
            }
            // adding the data to the .tsv file
            for (VcfLine aData : data) {
                if(aData.getMapKey()==key) {
                    writer.write("\n");
                    writer.write(aData.toString());
                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*****************************
     * updateDB - converts the VCFdata to Eva objects then does set operations
     * @param VCFdata - the data from the VCF file
     * @throws Exception
     *****************************/
    public void updateDB(ArrayList<VcfLine> VCFdata, int key) throws Exception {
        ArrayList<Eva> incomingData = new ArrayList<>();
        convertToEva(incomingData, VCFdata);
        insertAndDeleteEvaObjectsbyKey(incomingData, key);
    }

    /*****************************
     * convertToEva - converts the VCFdata into Eva objects
     * @param VCFtoEva - empty list that gets filled with new Eva objects
     * @param VCFdata  - the incoming data to be converted
     *****************************/
    public void convertToEva(ArrayList<Eva> VCFtoEva, ArrayList<VcfLine> VCFdata) {
        for (VcfLine e : VCFdata) {
            Eva temp = new Eva();
            temp.setChromosome(e.getChrom());
            temp.setPos(e.getPos());
            temp.setRsid(e.getID());
            temp.setRefnuc(e.getRef());
            temp.setVarnuc(e.getAlt());
            temp.setSoterm(e.getInfo());
            temp.setMapkey(e.getMapKey());
            VCFtoEva.add(temp);
        }
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

    /*****
     * Function was taken from   https://www.journaldev.com/924/java-download-file-url
     * Function serves to download data from a given url
     *****/
    private void downloadUsingNIO(String urlStr, String file) throws IOException {
        URL url = new URL(urlStr);
        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
        rbc.close();
    }

    /*****
     * Function was taken from   https://www.journaldev.com/966/java-gzip-example-compress-decompress-file
     * Function serves to decompress the .gz file
     *****/
    private void decompressGzipFile(String gzipFile, String newFile) {
        try {
            FileInputStream fis = new FileInputStream(gzipFile);
            GZIPInputStream gis = new GZIPInputStream(fis);
            FileOutputStream fos = new FileOutputStream(newFile);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) != -1) {
                fos.write(buffer, 0, len);
            }
            //close resources
            fos.close();
            gis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

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