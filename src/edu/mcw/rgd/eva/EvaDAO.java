package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.AbstractDAO;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.sql.Types;
import java.util.List;

/**
 * Created by llamers on 1/23/2020.
 */
public class EvaDAO extends AbstractDAO{
    public EvaDAO(){}

    public List<Eva> getActiveArrayIdEvaFromEnsembl( int subStart, int subEnd) throws Exception {
        String query  = "select * from (Select EVA_ID, CHROMOSOME, POS, RS_ID, REF_NUC, VAR_NUC, SO_TERM_ACC, MAP_KEY," +
                " ROW_NUMBER() over (ORDER BY EVA_ID asc) as RowNo from EVA) t where RowNo between ? AND ?";
//         "select a.*, r.MAP_KEY from EVA a, MAPS r where a.MAP_KEY=r.MAP_KEY";
        return EvaQuery.execute(this, query, subStart, subEnd);
    }
    public int deleteEva(int EvaKey) throws Exception{
        String sql = "DELETE FROM EVA WHERE EVA_ID=?";
        return update(sql, EvaKey);
    }

    public int insertEva(List<Eva> Evas) throws Exception {
        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(), "INSERT INTO EVA (EVA_ID, CHROMOSOME, POS, RS_ID, " +
                "REF_NUC, VAR_NUC, SO_TERM_ACC, MAP_KEY) SELECT ?,?,?,?,?,?,?,? FROM dual" +
                " WHERE NOT EXISTS(SELECT 1 FROM EVA WHERE CHROMOSOME=? AND POS=? AND RS_ID=? AND REF_NUC=? AND VAR_NUC=?)",
                new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR});
        su.compile();

        int Evaid = this.getNextKeyFromSequence("EVA_SEQ");
        for( Eva eva: Evas ) {
            eva.setEvaid(Evaid++);

            su.update(eva.getEvaid(), eva.getChromosome(), eva.getPos(), eva.getRsid(),eva.getRefnuc(),
                    eva.getVarnuc(), eva.getSoterm(), eva.getMapkey(), eva.getChromosome(),
                    eva.getPos(), eva.getRsid(), eva.getRefnuc(), eva.getVarnuc());
        }

        return executeBatch(su);
    }

}
