package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.Variant;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
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
    private Map<Integer, Integer> sampleIds;
    private Map<Integer, Integer> currSampleIds;

    protected Logger logger = LogManager.getLogger("status");
    protected Logger updatedRsId = LogManager.getLogger("updateRsIds");

    private DAO dao = new DAO();
    private boolean deleteOldIds = false;
    private int totalInserted = 0, totalDeleted = 0;
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Map<Integer, Integer> releaseSamples = new HashMap<>();

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
                releaseSamples = getCurrSampleIds();
                deleteOldIds = false;
                importEVA(mapKeys,releaseVer);
            }
            else if (args[i].equals("-pastRelease")){
                releaseVer = getPastRelease();
                mapKeys = getPastRelease().keySet();
                releaseSamples = getSampleIds();
                importEVA(mapKeys,releaseVer);
            }
        }

        logger.info("Total EVA pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));

    }

    void importEVA(Set<Integer> mapKeys, Map<Integer, String> releaseVer) throws Exception {
        for (Integer mapKey : mapKeys) {

            geneCacheMap = new HashMap<>();
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
//        updateVariantTableRsIds(incoming);
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        if (!tobeInserted.isEmpty()) {
            logger.info("       New EVA objects to be inserted in chromosome "+chromosome+": " + tobeInserted.size());
            totalInserted += tobeInserted.size();
//            updateVariantTableRsIds(tobeInserted);
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
        String release = "release_2";
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

    void updateVariantTableRsIds(Collection<Eva> incoming) throws Exception{
        List<VariantMapData> evaVmd = new ArrayList<>();
        List<VariantMapData> updateEvaVmd = new ArrayList<>();
        List<VariantMapData> updateEvaV = new ArrayList<>();
        List<VariantSampleDetail> evaVsd = new ArrayList<>();

        // check location
        for (Eva e : incoming) {
            List<VariantMapData> data = dao.getVariant(e);

            if (!data.isEmpty()){// if exist update rsID
            // do a check on var_nuc for data
                boolean found = false;
                for (VariantMapData vmd : data){
                    boolean diffGenic = false;
                    if(Utils.stringsAreEqual(vmd.getVariantNucleotide(),e.getVarNuc()) &&
                            Utils.stringsAreEqual(vmd.getReferenceNucleotide(),e.getRefNuc()) &&
                            Utils.stringsAreEqual(vmd.getPaddingBase(),e.getPadBase()) ) {
                        // check for sample detail, add if not there
                        String genicStatus = isGenic(e.getMapkey(),e.getChromosome(),e.getPos()) ? "GENIC":"INTERGENIC";
                        if ( !Utils.stringsAreEqual(genicStatus, vmd.getGenicStatus()) ) {
                            vmd.setGenicStatus(genicStatus);
                            diffGenic = true;
                        }
                        if (!Utils.stringsAreEqual(vmd.getRsId(),e.getRsId()) ){
                            if (vmd.getRsId()==null || vmd.getRsId().equals(".")) {
                                vmd.setRsId(e.getRsId());
                                updateEvaV.add(vmd);
                            }
                            else{
                                updatedRsId.debug("Variant rgd_id="+vmd.getId()+"|Old rs_id="+vmd.getRsId()+"|New rs_id="+e.getRsId());
                            }
                        }
                        if (diffGenic)
                            updateEvaVmd.add(vmd);

                        // check if in sample detail, if not create new
                        List<VariantSampleDetail> sampleDetailInRgd = dao.getVariantSampleDetail((int)vmd.getId(),releaseSamples.get(e.getMapkey()));
                        if (sampleDetailInRgd.isEmpty()) {
                            evaVsd.add(createNewEvaVariantSampleDetail(e, vmd));
                        }
                        found = true;
                        break;
                    }
                }
                if (!found){
//                    System.out.println(""+e.dump("|"));
                    // add new variant or do a cnt in the loop, if none, create new variant
                        VariantMapData vmd = createNewEvaVariantMapData(e);
                        VariantSampleDetail vsd = createNewEvaVariantSampleDetail(e, vmd);
                        evaVmd.add(vmd);
                        evaVsd.add(vsd);
                }
                // do a check for RGD_ID

                // add to variant_sample_detail with eva sample leave zygosity stuff empty
//                    System.out.println(data.size() + "|Start|" + data.get(0).getStartPos() + "|Chromosome|" + data.get(0).getChromosome());
                // update rsId
                // add to variant_sample_detail with eva sample leave zygosity stuff empty


            }
            else{ // else add new line with  eva sample
//                System.out.println("New variant: "+e.dump("|"));
                VariantMapData vmd = createNewEvaVariantMapData(e);
                VariantSampleDetail vsd = createNewEvaVariantSampleDetail(e, vmd);
                evaVmd.add(vmd);
                evaVsd.add(vsd);
            }

        } // end for

        // insert/update data
        if (!updateEvaVmd.isEmpty()) {
            logger.info("           Variants Genic Status being updated: "+updateEvaVmd.size());
            dao.updateVariantMapData(updateEvaVmd);
        }
        if (!updateEvaV.isEmpty()){
            logger.info("           Variants being updated: "+updateEvaV.size());
            dao.updateVariant(updateEvaV);
        }
        if (!evaVmd.isEmpty()) {
            logger.info("           New EVA Variants being added: "+evaVmd.size());
            dao.insertVariants(evaVmd);
            dao.insertVariantMapData(evaVmd);
        }
        if (!evaVsd.isEmpty()) {
            logger.info("           Total variant samples being made:"+evaVsd.size());
            dao.insertVariantSample(evaVsd);
        }

    }

    public Variant createNewEvaVariant(Eva e) throws Exception{
        Variant v = new Variant();
        v.setChromosome(e.getChromosome());
        v.setReferenceNucleotide(e.getRefNuc());
        v.setVariantNucleotide(e.getVarNuc());
        v.setPaddingBase(e.getPadBase());
        v.setDepth(9);
        v.setVariantFrequency(1);
        v.setRsId(e.getRsId());
        return v;
    }

    public VariantMapData createNewEvaVariantMapData(Eva e) throws Exception{
        VariantMapData vmd = new VariantMapData();
        int speciesKey= SpeciesType.getSpeciesTypeKeyForMap(e.getMapkey());
//        RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by EVA pipeline", e.getMapkey());
//        vmd.setId(r.getRgdId());
        vmd.setRsId(e.getRsId());
        vmd.setSpeciesTypeKey(speciesKey);
        vmd.setVariantType(dao.getVariantType(e.getSoTerm()).toLowerCase());
        vmd.setChromosome(e.getChromosome());
        vmd.setStartPos(e.getPos());
        vmd.setPaddingBase(e.getPadBase());
        vmd.setReferenceNucleotide(e.getRefNuc());
        vmd.setVariantNucleotide(e.getVarNuc());
        vmd.setGenicStatus( isGenic(e.getMapkey(),e.getChromosome(),e.getPos()) ? "GENIC":"INTERGENIC" );
        if (e.getRefNuc()==null)
            vmd.setEndPos(e.getPos()+1);
        else
            vmd.setEndPos(e.getPos()+e.getRefNuc().length());
        vmd.setMapKey(e.getMapkey());
        return vmd;
    }

    public VariantSampleDetail createNewEvaVariantSampleDetail(Eva e, VariantMapData vmd) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail();
        vsd.setId(vmd.getId());
        vsd.setSampleId(releaseSamples.get(e.getMapkey()));
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
    }

    boolean isGenic(int mapKey, String chr, int pos) throws Exception {

        GeneCache geneCache = geneCacheMap.get(chr);
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(chr, geneCache);
            geneCache.loadCache(mapKey, chr, DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(pos);
        return !geneRgdIds.isEmpty();
    }
    Map<String, GeneCache> geneCacheMap;

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

    public void setSampleIds(Map<Integer, Integer> sampleIds) {
        this.sampleIds = sampleIds;
    }

    public Map<Integer, Integer> getSampleIds() {
        return sampleIds;
    }

    public void setCurrRelease(Map<Integer, String> incomingFiles2) {
        this.currRelease = incomingFiles2;
    }

    public Map<Integer, String> getCurrRelease() {
        return currRelease;
    }

    public void setCurrSampleIds(Map<Integer,Integer> currSampleIds) {
        this.currSampleIds = currSampleIds;
    }

    public Map<Integer,Integer> getCurrSampleIds() {
        return currSampleIds;
    }
}
