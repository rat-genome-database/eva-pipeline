package edu.mcw.rgd.eva;

import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class EvaImport {
    private String version;
    private Map<Integer, String> pastRelease;
    private Map<Integer, String> currRelease;


    protected Logger logger = LogManager.getLogger("status");

    protected Logger scaffoldsLog = LogManager.getLogger("scaffolds");
    protected Logger multiPos = LogManager.getLogger("multiPos");

    private DAO dao = new DAO();
    private boolean deleteOldIds = false;
    private int totalInserted = 0, totalDeleted = 0;
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public void run(String[] args) throws Exception{
        Set<Integer> mapKeys = getPastRelease().keySet();
        Map<Integer,String> releaseVer = new HashMap<>();

        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        for (int i = 1; i<args.length;i++){
            if ( args[i].equals("-newRelease")) {
                deleteOldIds = true;
            }
            if ( args[i].equals("-currentRel") ){
                releaseVer = getCurrRelease();
                mapKeys = getCurrRelease().keySet();
//                releaseSamples = getCurrSampleIds();
                deleteOldIds = false;
                importEVA(mapKeys,releaseVer);
            }
            else if (args[i].equals("-pastRelease")){
                releaseVer = getPastRelease();
                mapKeys = getPastRelease().keySet();
//                releaseSamples = getSampleIds();
                importEVA(mapKeys,releaseVer);
            }
        }

        logger.info("Total EVA pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));

    }

    void importEVA(Set<Integer> mapKeys, Map<Integer, String> releaseVer) throws Exception {
        for (Integer mapKey : mapKeys) {

            long timeStart = System.currentTimeMillis();
            edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
            String assemblyName = assembly.getName();
            logger.info("   Assembly "+assemblyName+" started at "+sdt.format(new Date(timeStart)));
            String localFile = downloadEvaVcfFile(releaseVer.get(mapKey), mapKey);
            extractData(localFile, mapKey);
            logger.info("   Finished updating database for assembly "+assemblyName);
            logger.info("   Total EVA objects removed:  "+totalDeleted);
            logger.info("   Total EVA objects inserted: "+totalInserted);
            logger.info("   EVA Assembly "+assemblyName+" -- elapsed time: "+
                    Utils.formatElapsedTime(timeStart,System.currentTimeMillis())+"\n");
            totalDeleted = 0;
            totalInserted = 0;
            removeMultiPositionVariants(mapKey);
        }
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
            if (lineData.contains("scaffold") || lineData.contains("unloc")){
                scaffoldsLog.debug(lineData);
                continue;
            }
            VcfLine vcf = new VcfLine(lineData, col, mapKey);

            VCFdata.add(vcf); // adds the line to the array list
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
        logger.info("   Total EVA objects checked: "+totalObjects);
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
        if (checkDuplicates(inRGD)) {
            logger.info("Duplicates were found");
        }

        logger.info("       Incoming EVA objects in chromosome "+chromosome+": " + incoming.size());
        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        if (!tobeInserted.isEmpty()) {
            logger.info("       New EVA objects to be inserted in chromosome "+chromosome+": " + tobeInserted.size());
            totalInserted += tobeInserted.size();
            dao.insertEva(tobeInserted);
            tobeInserted.clear();
        }

        // determines old objects to be deleted
        if (deleteOldIds) {
            Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);
            if (!tobeDeleted.isEmpty()) {
                logger.info("       Old EVA objects to be deleted in chromosome " + chromosome + ": " + tobeDeleted.size());
                totalDeleted += tobeDeleted.size();
                // delete from variants table, then set rgd_id status to withdrawn
                dao.deleteEvaBatch(tobeDeleted);
                tobeDeleted.clear();
            }
        }
        Collection<Eva> matching = CollectionUtils.intersection(inRGD, incoming);
        int matchingEVA = matching.size();
        if (matchingEVA != 0) {
            logger.info("       EVA objects that are matching in chromosome "+chromosome+": " + matchingEVA);
            matching.clear();
        }
    }
    String downloadEvaVcfFile(String file, int key) throws Exception{
        String[] fileSplit = file.split("/");
        String release = "release_3";
        for (String s : fileSplit){
            if (s.contains("release_")) {
                release = s;
                break;
            }
        }
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile("data/EVA_"+key+"_"+release+".vcf");
        downloader.setUseCompression(true);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
    }

    boolean checkDuplicates(List<Eva> inRgd) throws Exception{
        List<Eva> copy = new ArrayList<>();
        Set<Eva> hs = new LinkedHashSet<>();
        hs.addAll(inRgd);
        copy.addAll(hs); // removes duplicates
        Collection<Eva> dupeDelete = CollectionUtils.subtract(inRgd,copy);
        if( !dupeDelete.isEmpty() ) {
            dao.deleteEvaBatch(dupeDelete);
            logger.warn("total duplicates "+dupeDelete.size());
            inRgd.clear();
            inRgd.addAll(copy);
            return true;
        }

        return false;
    }


    void removeMultiPositionVariants(int mapKey) throws Exception{
        List<String> rsIDs = dao.getMultiMappedrsId(mapKey);
//        List<VariantMapData> deleteMe = new ArrayList<>();
//        // delete variants with rsIds
//        for (String rsId : rsIDs){
//            multiPos.debug("rs ID to be removed: "+rsId);
//            List<VariantMapData> subset = dao.getVariantByRsId(rsId,mapKey);
//            List<Eva> subset = dao.getEvaObjectsByRsId(rsId,mapKey);
//            deleteMe.addAll(subset);
//        }
        if (!rsIDs.isEmpty()){
            logger.info("\tEVA objects mapped to multiple positions being withdrawn: "+rsIDs.size()+"\n");
            dao.deleteEvaBatchByRsId(rsIDs, mapKey);
        }
    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setPastRelease(Map<Integer, String> incomingFiles) {
        this.pastRelease = incomingFiles;
    }

    public Map<Integer, String> getPastRelease() {
        return pastRelease;
    }

    public void setCurrRelease(Map<Integer, String> incomingFiles2) {
        this.currRelease = incomingFiles2;
    }

    public Map<Integer, String> getCurrRelease() {
        return currRelease;
    }
}
