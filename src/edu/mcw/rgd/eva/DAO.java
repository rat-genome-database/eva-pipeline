package edu.mcw.rgd.eva;

//import edu.mcw.rgd.dao.impl.EvaDAO;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    EvaDAO edao = new EvaDAO();
    Logger logInserted = Logger.getLogger("inserted");
    Logger logDeleted = Logger.getLogger("deleted");

    public DAO() {}

    public String getConnection(){
        return edao.getConnectionInfo();
    }

    public List<Eva> getEvaObjectsFromMapKey(int mapKey) throws Exception {
        return edao.getEvaObjectsFromMapKey(mapKey);
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
}
