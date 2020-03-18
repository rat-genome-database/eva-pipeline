package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.impl.EvaDAO;
import edu.mcw.rgd.datamodel.Eva;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    EvaDAO edao = new EvaDAO();
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
    public void CalcPadBase(ArrayList<Eva> EvaData) {
        for(Eva eva : EvaData) {
            // check if size is equal
            // if not, get pad base by checking smaller size nucleotide and remove it from var or ref
            // null out smaller nucleotide

            String soTerm = eva.getSoTerm();
            switch (soTerm) {
                case "0000159":
                    String varnuc = eva.getVarNuc();
                    int refSize = eva.getRefNuc().length();
                    String newRef = eva.getRefNuc().substring(1,refSize);
                    int pos = eva.getPos()+1;
                    eva.setRefnuc(newRef);
                    eva.setPadBase(varnuc);
                    eva.setVarnuc(null);
                    eva.setPos(pos);
                    break;
                case "0000667":
                    String refnuc = eva.getRefNuc();
                    int varSize = eva.getVarNuc().length();
                    String newVar = eva.getVarNuc().substring(1,varSize);
                    int pos2 = eva.getPos()+1;
                    eva.setVarnuc(newVar);
                    eva.setPadBase(refnuc);
                    eva.setRefnuc(null);
                    eva.setPos(pos2);
                    break;
                case "0002007":
                    eva.setPadBase(null);
                    break;
                case "1000032":
                    eva.setPadBase(null);
                    break;
                case "0000705":
                    eva.setPadBase(null);
                    break;
                default:
                    eva.setPadBase(null);
                    break;
            }

        }

    }
}
