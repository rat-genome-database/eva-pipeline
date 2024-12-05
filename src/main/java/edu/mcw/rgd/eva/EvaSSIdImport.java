package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSSId;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map;

public class EvaSSIdImport {
    private String version;
    private String directory;
    private int mapKey;
    protected Logger logger = LogManager.getLogger("status");
    private DAO dao = new DAO();
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Map<String, GeneCache> geneCacheMap;
    Map<String, Sample> sampleMap = new HashMap<>();

    public void run(String[] args) throws Exception{

        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        try {
            geneCacheMap = new HashMap<>();
            File folder = new File(directory);
            List<File> files = listFilesInFolder(folder);
            for (File f : files){
                extractData(f);
            }
        } catch (Exception e) {
            Utils.printStackTrace(e, logger);
        }

        logger.info("Total EVA pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));

    }

    public void extractData(File file) throws Exception {
        String[] col = null;
        logger.debug("  Extracting data from "+file.getName());
        String name = getStrainName(file.getName());
        Integer strainRgdId = getStrainRgdId(name);
        Sample sample = dao.getSampleByAnalysisNameAndMapKey(name,mapKey);
        sampleMap.put(name,sample);
        BufferedReader br = dao.openFile(file.getAbsolutePath());
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
            if (lineData.contains("scaffold") || lineData.contains("unloc") || lineData.contains("Contig") ||
                    lineData.contains("AF0") || lineData.contains("Scaffold") || lineData.contains("GL45") ||
                    lineData.contains("RANDOM") || lineData.contains("MSCH")){
                continue;
            }

            VcfLine vcf = new VcfLine(lineData, col, mapKey);
//            List<VcfLine> vcfs = vcf.parse(lineData, col, mapKey);
            if (vcf.getID().contains(";")) {
                continue;
            }
            VCFdata.add(vcf);// adds the line to the array list
            // go until chromosome changes
        } // end while
//        List<VcfLine> VCFbyChrom = VCFdata.subList(0,i);
        updateDB(VCFdata, name);
//        logger.info("   Total EVA objects checked: "+totalObjects);
        br.close();
    }

    public void updateDB(List<VcfLine> VCFdata, String strainName) throws Exception {
        ArrayList<Eva> incomingData = new ArrayList<>();
        dao.convertToEva(incomingData, VCFdata);
        dao.calcPaddingBaseNoSOTerm(incomingData);
        insertIntoVariantTables(incomingData,strainName);
//        insertAndDeleteEvaObjectsByKeyAndChromosome(incomingData, mapKey, chromosome);
        // make new insertDelete to handle making variants like variant import
    }

    public void insertIntoVariantTables(List<Eva> incoming, String strainName) throws Exception{
        List<VariantMapData> evaVmd = new ArrayList<>();
        List<VariantMapData> updateEvaVmd = new ArrayList<>();
        List<VariantSampleDetail> evaVsd = new ArrayList<>();
        List<VariantSSId> ssIds = new ArrayList<>();
        Sample s = sampleMap.get(strainName);
        // check location
        for (Eva e : incoming) {
            List<VariantMapData> data = dao.getVariant(e); // do a check for RGD_ID

            if (!data.isEmpty()){// if exist update rsID
                // do a check on var_nuc for data
                boolean found = false;
                for (VariantMapData vmd : data){
                    if(Utils.stringsAreEqual(vmd.getVariantNucleotide(),e.getVarNuc()) &&
                            Utils.stringsAreEqual(vmd.getReferenceNucleotide(),e.getRefNuc()) &&
                            Utils.stringsAreEqual(vmd.getPaddingBase(),e.getPadBase()) ) {
                        // check for sample detail, add if not there
                        String genicStatus = isGenic(vmd) ? "GENIC":"INTERGENIC";
                        if ( !Utils.stringsAreEqual(genicStatus, vmd.getGenicStatus()) || Utils.isStringEmpty(vmd.getGenicStatus()) ) {
                            vmd.setGenicStatus(genicStatus);
                            updateEvaVmd.add(vmd);
                        }

                        // check if in sample detail, if not create new
                        List<VariantSampleDetail> sampleDetailInRgd = dao.getVariantSampleDetail((int)vmd.getId(),s.getId());
                        if (sampleDetailInRgd.isEmpty()) {
                            evaVsd.add(createNewEvaVariantSampleDetail(vmd,s.getId()));
                        }
                        VariantSSId ssid = dao.getVariantSSIdByRgdIdSSId((int)vmd.getId(),e.getRsId());
                        // check if exists
                        if (ssid == null) {
                            ssid = new VariantSSId();
                            ssid.setVariantRgdId((int) vmd.getId());
                            ssid.setSSId(e.getRsId());
                            ssid.setStrainRgdId(s.getStrainRgdId());
                            ssIds.add(ssid);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found){
//                    System.out.println(""+e.dump("|"));
                    // add new variant or do a cnt in the loop, if none, create new variant
                    VariantMapData vmd = createNewEvaVariantMapData(e);
                    VariantSampleDetail vsd = createNewEvaVariantSampleDetail(vmd,s.getId());
                    VariantSSId ssid = new VariantSSId();
                    ssid.setVariantRgdId((int)vmd.getId());
                    ssid.setSSId(e.getRsId());
                    ssid.setStrainRgdId(s.getStrainRgdId());
                    ssIds.add(ssid);
                    evaVmd.add(vmd);
                    evaVsd.add(vsd);
                }
//                    System.out.println(data.size() + "|Start|" + data.get(0).getStartPos() + "|Chromosome|" + data.get(0).getChromosome());
            }
            else{ // else add new line with  eva sample
//                System.out.println("New variant: "+e.dump("|"));
                VariantMapData vmd = createNewEvaVariantMapData(e);
                VariantSampleDetail vsd = createNewEvaVariantSampleDetail(vmd,s.getId());
                VariantSSId ssid = new VariantSSId();
                ssid.setVariantRgdId((int)vmd.getId());
                ssid.setSSId(e.getRsId());
                ssid.setStrainRgdId(s.getStrainRgdId());
                ssIds.add(ssid);
                evaVmd.add(vmd);
                evaVsd.add(vsd);
            }

        }
        // insert things
        if (!updateEvaVmd.isEmpty()) {
            logger.info("\t\t\tVariants Genic Status being updated: "+updateEvaVmd.size());
            dao.updateVariantMapData(updateEvaVmd);
        }
        if (!evaVmd.isEmpty()) {
            logger.info("\t\t\tNew EVA Variants being added: "+evaVmd.size());
            dao.insertVariantRgdIds(evaVmd);
            dao.insertVariants(evaVmd);
            dao.insertVariantMapData(evaVmd);
        }
        if (!evaVsd.isEmpty()) {
            logger.info("\t\t\tTotal variant samples being made: "+evaVsd.size());
            dao.insertVariantSample(evaVsd);
        }
        if (!ssIds.isEmpty()){
            logger.info("\t\t\tInserting variant ssIds: "+ssIds.size());
            dao.insertVariantSSIds(ssIds);
        }
    }

    List<File> listFilesInFolder(File folder) throws Exception {
        return Arrays.asList(Objects.requireNonNull(folder.listFiles()));
    }

    boolean isGenic(VariantMapData vmd) throws Exception {

        GeneCache geneCache = geneCacheMap.get(vmd.getChromosome());
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(vmd.getChromosome(), geneCache);
            geneCache.loadCache(vmd.getMapKey(), vmd.getChromosome(), DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds((int)vmd.getStartPos(),(int)vmd.getEndPos());
        return !geneRgdIds.isEmpty();
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
        RgdId r = dao.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", "created by EVA pipeline", e.getMapkey());
        vmd.setId(r.getRgdId());
//        vmd.setRsId(e.getRsId());
        vmd.setSpeciesTypeKey(speciesKey);
        vmd.setVariantType(dao.getVariantType(e.getSoTerm()).toLowerCase());
        vmd.setChromosome(e.getChromosome());
        vmd.setStartPos(e.getPos());
        vmd.setPaddingBase(e.getPadBase());
        vmd.setReferenceNucleotide(e.getRefNuc());
        vmd.setVariantNucleotide(e.getVarNuc());
        if (e.getRefNuc()==null)
            vmd.setEndPos(e.getPos()+1);
        else
            vmd.setEndPos(e.getPos()+e.getRefNuc().length());
        vmd.setMapKey(e.getMapkey());
        String genicStat = isGenic(vmd) ? "GENIC":"INTERGENIC";
        vmd.setGenicStatus(genicStat);
        return vmd;
    }

    String getStrainName(String fileName) throws Exception{
        String strain = fileName;
        if (strain.contains("_PASS"))
        {
            strain = strain.replace("_PASS","");
        }
        int lastUnderScore = strain.lastIndexOf('_');
        strain = strain.substring(0,lastUnderScore);
        lastUnderScore = strain.lastIndexOf('_');
        strain = strain.substring(0,lastUnderScore)+")";
        int cnt =0;
        for (int i = 0; i < strain.length(); i++){
            if (strain.charAt(i) == '_'){
                cnt++;
            }
        }
        if (cnt>2) {
            strain = strain.replaceFirst("_", "-");
        }
        strain = strain.replaceFirst("_","/");
        strain = strain.replace("_"," (");

        return strain;
    }

    Integer getStrainRgdId(String sampleName) throws Exception {
        int strainStop = sampleName.indexOf('(')-1;
        String strainName = sampleName.substring(0,strainStop);
        return dao.getStrainRgdIdByTaglessStrainSymbol(strainName);
    }

    public VariantSampleDetail createNewEvaVariantSampleDetail(VariantMapData vmd, int sampleId) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail(); // add to variant_sample_detail with eva sample leave zygosity stuff empty
        vsd.setId(vmd.getId());
        vsd.setSampleId(sampleId);
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getDirectory() {
        return directory;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public int getMapKey() {
        return mapKey;
    }
}
