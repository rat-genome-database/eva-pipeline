package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.impl.EvaDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    EvaDAO edao = new EvaDAO();
    MapDAO mapDAO = new MapDAO();
    Logger logInserted = Logger.getLogger("insertedEva");
    Logger logDeleted = Logger.getLogger("deletedEva");

    public DAO() {}

    public String getConnection(){
        return edao.getConnectionInfo();
    }

    public List<Eva> getEvaObjectsFromMapKey(int mapKey) throws Exception {
        return edao.getEvaObjectsFromMapKey(mapKey);
    }
    public List<Eva> getEvaObjectsFromMapKeyAndChromosome(int mapKey, String chromosome) throws Exception{
        return edao.getEvaObjectsFromMapKeyAndChromosome(mapKey,chromosome);
    }
    public int deleteEva(int EvaKey) throws Exception{
        return edao.deleteEva(EvaKey);
    }
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
            // check if size is equal
            // if not, get pad base by checking smaller size nucleotide and remove it from var or ref
            // null out smaller nucleotide
            if(eva.getSoTerm()==null) {
                eva.setPadBase(null);
                continue;
            }
            String soTerm = eva.getSoTerm();
            switch (soTerm) {
                case "SO:0000159":
                case "0000159": // deletion
                    String varnuc = eva.getVarNuc();
                    int refSize = eva.getRefNuc().length();
                    String newRef = eva.getRefNuc().substring(1,refSize);
                    int pos = eva.getPos()+1;
                    eva.setRefnuc(newRef);
                    eva.setPadBase(varnuc);
                    eva.setVarnuc(null);
                    eva.setPos(pos);
                    break;
                case "SO:0000667":
                case "0000667": // insertion
                    String refnuc = eva.getRefNuc();
                    int varSize = eva.getVarNuc().length();
                    String newVar = eva.getVarNuc().substring(1,varSize);
                    int pos2 = eva.getPos()+1;
                    eva.setVarnuc(newVar);
                    eva.setPadBase(refnuc);
                    eva.setRefnuc(null);
                    eva.setPos(pos2);
                    break;
                case "0002007": // MNV
                    eva.setPadBase(null);
                    break;
                case "1000032": // delin, indel
                    eva.setPadBase(null);
                    break;
                case "0000705": // tandem repeat
                    eva.setPadBase(null);
                    break;
                default:
                    eva.setPadBase(null);
                    break;
            }

        }

    }
}
