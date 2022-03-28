package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.EvaDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.spring.variants.VariantMapQuery;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    EvaDAO edao = new EvaDAO();
    OntologyXDAO xdao = new OntologyXDAO();
    private RGDManagementDAO managementDAO = new RGDManagementDAO();
    Logger logInserted = LogManager.getLogger("insertedEva");
    Logger logDeleted = LogManager.getLogger("deletedEva");
    Logger updatedRsId = LogManager.getLogger("updateRsIds");
    Logger newVariants = LogManager.getLogger("newVariants");
    Logger evaSampleDetails = LogManager.getLogger("evaSampleDetails");

    public String getConnection(){
        return edao.getConnectionInfo();
    }

    /*
    public List<Eva> getEvaObjectsFromMapKey(int mapKey) throws Exception {
        return edao.getEvaObjectsFromMapKey(mapKey);
    }
    */

    public List<Eva> getEvaObjectsFromMapKeyAndChromosome(int mapKey, String chromosome) throws Exception{
        return edao.getEvaObjectsFromMapKeyAndChromosome(mapKey,chromosome);
    }

    /*
    public int deleteEva(int EvaKey) throws Exception{
        return edao.deleteEva(EvaKey);
    }
    */

    public void deleteEvaBatch(Collection<Eva> tobeDeleted) throws Exception {
        for(Eva eva : tobeDeleted)
            logDeleted.debug(eva.dump("|"));
        edao.deleteEvaBatch(tobeDeleted);
    }
    public int insertEva(Collection<Eva> tobeInserted) throws Exception {
        for(Eva eva : tobeInserted)
            logInserted.debug(eva.dump("|"));
        return edao.insertEva(tobeInserted);
    }

    public java.util.Map<String,Integer> getChromosomeSizes(int mapKey) throws Exception {
        MapDAO dao = new MapDAO();
        return dao.getChromosomeSizes(mapKey);
    }
    /*****************************
     * convertToEva - converts the VCFdata into Eva objects
     * @param VCFtoEva - empty list that gets filled with new Eva objects
     * @param VCFdata  - the incoming data to be converted
     *****************************/
    public void convertToEva(ArrayList<Eva> VCFtoEva, List<VcfLine> VCFdata) {
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


    public void convertAPIToEva(ArrayList<Eva> eva, List<EvaAPI> api) throws Exception{
        for (EvaAPI e : api) {
            if(e.getRefAllele().equals(e.getAllele())){
                continue;
            }

            Eva temp = new Eva();
            temp.setChromosome(e.getChromosome());
            temp.setPos(e.getPosition());
            temp.setRsid(e.getEvaName());
            temp.setRefnuc(e.getRefAllele());
            temp.setVarnuc(e.getAllele());
            temp.setMapkey(e.getMapKey());
            String soType = e.getSnpClass().toLowerCase();
            switch (soType)
            {
                case("snv"):
                    temp.setSoterm("SO:0001483");
                    break;
                case("del"):
                case("deletion"):
                case("deleted_sequence"):
                case("nucleotide_deletion"):
                    temp.setSoterm("SO:0000159");
                    break;
                case("insertion"):
                case("nucleotide_insertion"):
                case("ins"):
                    temp.setSoterm("SO:0000667");
                    break;
                case("mnv"):
                    temp.setSoterm("SO:0002007");
                    break;
                case("delins"):
                case("deletion-insertion"):
                case("indel"):
                    temp.setSoterm("SO:1000032");
                    break;
                case("tandem_repeat"):
                    temp.setSoterm("SO:0000705");
                    break;
                case("sv"): // strucural variant
                    temp.setSoterm("SO:0001537");
                    break;
                default:
                    temp.setSoterm(null);
                    break;
            }
            eva.add(temp);
        }
        return;

    }
    public void CalcPadBase(ArrayList<Eva> EvaData) {
        for(Eva eva : EvaData) {
            if(eva.getSoTerm()==null) {
                eva.setPadBase(null);
                continue;
            }
            String soTerm = eva.getSoTerm();
            switch (soTerm) {
                case "SO:0000159":// deletion
                    String varnuc = eva.getVarNuc();
                    int refSize = eva.getRefNuc().length();
                    String newRef = eva.getRefNuc().substring(1,refSize);
                    int pos = eva.getPos()+1;
                    eva.setRefnuc(newRef);
                    eva.setPadBase(varnuc);
                    eva.setVarnuc(null);
                    eva.setPos(pos);
                    break;
                case "SO:0000667":// insertion
                    String refnuc = eva.getRefNuc();
                    int varSize = eva.getVarNuc().length();
                    String newVar = eva.getVarNuc().substring(1,varSize);
                    int pos2 = eva.getPos()+1;
                    eva.setVarnuc(newVar);
                    eva.setPadBase(refnuc);
                    eva.setRefnuc(null);
                    eva.setPos(pos2);
                    break;
                case "SO:0002007":// MNV
                    eva.setPadBase(null);
                    break;
                case "SO:1000032":// delin, indel
                    eva.setPadBase(null);
                    break;
                case "SO:0000705":// tandem repeat
                    eva.setPadBase(null);
                    break;
                default:
                    eva.setPadBase(null);
                    break;
            }

        }

    }
    public String getVariantType(String soTerm) throws Exception{
        Term t = xdao.getTermByAccId(soTerm);
        return t.getTerm();
    }
    public List<VariantMapData> getVariant(Eva e)throws Exception{
//        String query = "select v.*,vmd.chromosome,vmd.padding_base,vmd.start_pos,vmd.end_pos,vmd.genic_status, vmd.map_key" +
//                       " from variant v, variant_map_data vmd where v.rgd_id=vmd.rgd_id and vmd.map_key=? and vmd.chromosome=? and vmd.start_pos=?";
        String sql = "SELECT * FROM variant v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where vmd.map_key=? and vmd.chromosome=? and vmd.start_pos=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(e.getMapkey(), e.getChromosome(), e.getPos());
    }

    public void insertVariants(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql1 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant (\n" +
                        " RGD_ID,REF_NUC, VARIANT_TYPE, VAR_NUC, RS_ID, CLINVAR_ID, SPECIES_TYPE_KEY)\n" +
                        "VALUES (\n" +
                        "  ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,Types.INTEGER}, 10000);
        sql1.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql1.update(id, v.getReferenceNucleotide(), v.getVariantType(), v.getVariantNucleotide(), v.getRsId(), v.getClinvarId(), v.getSpeciesTypeKey());

        }
        sql1.flush();
    }
    public void insertVariantMapData(List<VariantMapData> mapsData)  throws Exception{
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_map_data (\n" +
                        " RGD_ID,CHROMOSOME,START_POS,END_POS,PADDING_BASE,GENIC_STATUS,MAP_KEY)\n" +
                        "VALUES (\n" +
                        " ?,?,?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,Types.VARCHAR, Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            newVariants.debug("Variant being inserted with RGD_ID=" + id);
            sql2.update(id, v.getChromosome(), v.getStartPos(), v.getEndPos(), v.getPaddingBase(), v.getGenicStatus(), v.getMapKey());
        }
        sql2.flush();
    }

    public int insertVariantSample(List<VariantSampleDetail> sampleData) throws Exception {
        BatchSqlUpdate bsu= new BatchSqlUpdate(this.getVariantDataSource(),
                "INSERT INTO variant_sample_detail (\n" +
                        " RGD_ID,SOURCE,SAMPLE_ID,TOTAL_DEPTH,VAR_FREQ,ZYGOSITY_STATUS,ZYGOSITY_PERCENT_READ," +
                        "ZYGOSITY_POSS_ERROR,ZYGOSITY_REF_ALLELE,ZYGOSITY_NUM_ALLELE,ZYGOSITY_IN_PSEUDO,QUALITY_SCORE)\n" +
                        "VALUES (?,?,?,?,?,?,?," +
                        "?,?,?,?,?)",
                new int[]{Types.INTEGER,Types.VARCHAR,Types.INTEGER, Types.INTEGER, Types.INTEGER,Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR,Types.VARCHAR, Types.INTEGER,Types.VARCHAR, Types.INTEGER}, 10000);
        bsu.compile();
        for(VariantSampleDetail v: sampleData ) {
            bsu.update(v.getId(), v.getSource(), v.getSampleId(),v.getDepth(),v.getVariantFrequency(),v.getZygosityStatus(),v.getZygosityPercentRead(),
                    v.getZygosityPossibleError(),v.getZygosityRefAllele(),v.getZygosityNumberAllele(),v.getZygosityInPseudo(),v.getQualityScore());
            evaSampleDetails.debug("New Variant sample detail being added for EVA: RGD_ID="+"|SAMPLE_ID="+v.getSampleId());
        }
        bsu.flush();
        // compute nr of rows affected
        int totalRowsAffected = 0;
        for( int rowsAffected: bsu.getRowsAffected() ) {
            totalRowsAffected += rowsAffected;
        }
        return totalRowsAffected;
    }

    public void updateVariantMapData(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "update variant set RS_ID=?, GENIC_STATUS=?  where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.VARCHAR, Types.INTEGER}, 10000);
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            updatedRsId.debug("Variant rsId and Genic status being updated with RGD_ID= " + id);
            sql2.update(v.getRsId(), v.getGenicStatus(), id);
        }
        sql2.flush();
    }

    public List<VariantSampleDetail> getVariantSampleDetail(int rgdId, int sampleId) throws Exception{
        String sql = "SELECT * FROM variant_sample_detail  WHERE rgd_id=? AND sample_id=?";
        VariantSampleQuery q = new VariantSampleQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(rgdId, sampleId);
    }

    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getCarpeNovoDataSource();
    }
    public RgdId createRgdId(int objectKey, String objectStatus, String notes, int mapKey) throws Exception{
        int speciesKey=SpeciesType.getSpeciesTypeKeyForMap(mapKey);
        return managementDAO.createRgdId(objectKey, objectStatus, notes, speciesKey);
    }
}
