package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map;

public class VariantImport {
    private DAO dao = new DAO();
    private Map<Integer, Integer> releaseSamples = new HashMap<>();
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Map<Integer, Integer> sampleIds;
    private Map<Integer, Integer> currSampleIds;
    private String version;

    protected Logger updatedRsId = LogManager.getLogger("updateRsIds");
    protected Logger logger = LogManager.getLogger("variantSummary");

    void run(String[] args) throws Exception{
        Set<Integer> mapKeys = getSampleIds().keySet();

        logger.info(getVersion());
        logger.info("   "+dao.getConnection());
        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        releaseSamples = getSampleIds();
        mapKeys = getSampleIds().keySet();
        for (int i = 1; i<args.length;i++){
//            if ( args[i].equals("-currentRel") ){
//                releaseSamples = getCurrSampleIds();
//                mapKeys = getCurrSampleIds().keySet();
//            }
//            else if (args[i].equals("-pastRelease")){
//                releaseSamples = getSampleIds();
//                mapKeys = getSampleIds().keySet();
//            }
            try {
                int mapKey = Integer.parseInt(args[i]);
//                releaseSamples = getCurrSampleIds();
                insertVariants(mapKey);
            }
            catch (Exception e){
                Utils.printStackTrace(e,logger);
            }

        }

        logger.info("Total EVA Variant import pipeline runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    void insertVariants(int mapKey) throws Exception{
        long timeStart = System.currentTimeMillis();
        edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
        String assemblyName = assembly.getName();
        geneCacheMap = new HashMap<>();
        logger.info("\tAssembly "+assemblyName+" started at "+sdt.format(new Date(timeStart)));
        loadEvaByChromosome(mapKey);
        logger.info("\tFinished updating Variant database for assembly "+assemblyName);
        logger.info("\tEVA Assembly "+assemblyName+" -- elapsed time: "+
                Utils.formatElapsedTime(timeStart,System.currentTimeMillis())+"\n");
    }



    void loadEvaByChromosome(int mapKey) throws Exception{
        Set<String> chromosomes = dao.getChromosomes(mapKey);

        for (String chrom : chromosomes){
            Collection<Eva> evas = dao.getEvaObjectsFromMapKeyAndChromosome(mapKey,chrom);
            if (!evas.isEmpty()){
                logger.info("\t\tChecking EVA objects to be entered into Variant tables for Chromosome "+chrom+": "+evas.size());
                updateVariantTableRsIds(evas);
            }
        }
    }

    void updateVariantTableRsIds(Collection<Eva> incoming) throws Exception{
        List<VariantMapData> evaVmd = new ArrayList<>();
        List<VariantMapData> updateEvaVmd = new ArrayList<>();
        List<VariantMapData> updateEvaV = new ArrayList<>();
        List<VariantSampleDetail> evaVsd = new ArrayList<>();

        // check location
        for (Eva e : incoming) {
            List<VariantMapData> data = dao.getVariant(e); // do a check for RGD_ID

            if (!data.isEmpty()){// if exist update rsID
                // do a check on var_nuc for data
                boolean found = false;
                for (VariantMapData vmd : data){
                    boolean diffGenic = false;
                    if(Utils.stringsAreEqual(vmd.getVariantNucleotide(),e.getVarNuc()) &&
                            Utils.stringsAreEqual(vmd.getReferenceNucleotide(),e.getRefNuc()) &&
                            Utils.stringsAreEqual(vmd.getPaddingBase(),e.getPadBase()) ) {
                        // check for sample detail, add if not there
                        String genicStatus = isGenic(vmd) ? "GENIC":"INTERGENIC";
                        if ( !Utils.stringsAreEqual(genicStatus, vmd.getGenicStatus()) || Utils.isStringEmpty(vmd.getGenicStatus()) ) {
                            vmd.setGenicStatus(genicStatus);
                            diffGenic = true;
                        }
                        if (!Utils.stringsAreEqual(vmd.getRsId(),e.getRsId()) ){
                            updatedRsId.debug("Variant rgd_id="+vmd.getId()+"|Old rs_id="+vmd.getRsId()+"|New rs_id="+e.getRsId()+"|");
                            vmd.setRsId(e.getRsId()); // update rsId
                            updateEvaV.add(vmd);
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
//                    System.out.println(data.size() + "|Start|" + data.get(0).getStartPos() + "|Chromosome|" + data.get(0).getChromosome());
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
            logger.info("\t\t\tVariants Genic Status being updated: "+updateEvaVmd.size());
            dao.updateVariantMapData(updateEvaVmd);
        }
        if (!updateEvaV.isEmpty()){
            logger.info("\t\t\tVariants being updated: "+updateEvaV.size());
            dao.updateVariant(updateEvaV);
            dao.deleteSSIdBatch(updateEvaV);
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
        vmd.setRsId(e.getRsId());
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

    public VariantSampleDetail createNewEvaVariantSampleDetail(Eva e, VariantMapData vmd) throws Exception{
        VariantSampleDetail vsd = new VariantSampleDetail(); // add to variant_sample_detail with eva sample leave zygosity stuff empty
        vsd.setId(vmd.getId());
        vsd.setSampleId(releaseSamples.get(e.getMapkey()));
        vsd.setDepth(9);
        vsd.setVariantFrequency(1);
        return vsd;
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

    Map<String, GeneCache> geneCacheMap;

    public void setSampleIds(Map<Integer,Integer> sampleIds) {
        this.sampleIds=sampleIds;
    }

    private Map<Integer, Integer> getSampleIds() {
        return sampleIds;
    }

    public void setCurrSampleIds(Map<Integer,Integer> currSampleIds) {
        this.currSampleIds=currSampleIds;
    }
    public Map<Integer, Integer> getCurrSampleIds(){
        return currSampleIds;
    }

    public void setVersion(String version) {
        this.version=version;
    }
    public String getVersion(){
        return version;
    }
}
