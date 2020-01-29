//package edu.mcw.rgd.dao.spring;
package edu.mcw.rgd.eva;

import edu.mcw.rgd.dao.AbstractDAO;
import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class EvaQuery extends MappingSqlQuery {

    public EvaQuery(DataSource ds, String query) {
        super(ds, query);
    }

    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {

        Eva eva = new Eva();
        eva.setEvaid(rs.getInt("EVA_ID"));
        eva.setChromosome(rs.getString("CHROMOSOME"));
        eva.setPos(rs.getInt("POS"));
        eva.setRsid(rs.getString("RS_ID"));
        eva.setRefnuc(rs.getString("REF_NUC"));
        eva.setVarnuc(rs.getString("VAR_NUC"));
        eva.setSoterm(rs.getString("SO_TERM_ACC"));
        eva.setMapkey(rs.getInt("MAP_KEY"));

        return eva;
    }

    public static List<Eva> execute(AbstractDAO dao, String sql, Object... params) throws Exception {
        EvaQuery q = new EvaQuery(dao.getDataSource(), sql);
        return dao.execute(q, params);
    }
}
