package com.olanola.cf;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.ReloadFromJDBCDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.JDBCDataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Home on 30.07.2014.
 */

public class MahoutPool {
    PGSimpleDataSource dataSource;

    public Recommender getRecommender() {
        return recommender;
    }

    public DataModel getDataModel() {
        return dataModel;
    }

    public UserNeighborhood getNeighborhood() {
        return neighborhood;
    }

    public UserSimilarity getUserSimilarity() {
        return userSimilarity;
    }

    Recommender recommender;
    DataModel dataModel;
    UserNeighborhood neighborhood;
    UserSimilarity userSimilarity;
    JDBCDataModel jdbcdatamodel;

    public MahoutPool()  throws IOException {

        dataSource = new PGSimpleDataSource();
        dataSource.setServerName("dbase1-3.mtml.ru");
        dataSource.setUser("oleg");
        dataSource.setPassword("f492ftg2409tfg2");
        dataSource.setDatabaseName("mirtesen");
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            Statement stmt = null;
            ResultSet rs = null;
            stmt = conn.createStatement();

            if (stmt != null) {
                rs = stmt.executeQuery("select p2o_person_obj_id, p2o_obj_obj_id,abs(COALESCE(p2o_mark,0))+CASE WHEN p2o_is_viewed then 1 else 0 end, p2o_modified " +
                        "from person2obj " +
                        "where not p2o_notify and not p2o_is_trash and p2o_obj_obj_id > 43000000000 order by p2o_id desc limit 5");
                while (rs.next()) {
                    String str = rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(3);
                    System.err.println(str);
                }
                try {
                    System.out.println("Start ReloadFromJDBCDataModel+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    jdbcdatamodel = new PostgreSQLJDBCDataModel(dataSource,
                            "public.person2obj",
                            "p2o_person_obj_id",
                            "p2o_obj_obj_id",
                            "abs(COALESCE(p2o_mark,0))+CASE WHEN p2o_is_viewed then 1 else 0 end ",
                            "p2o_modified");
                    System.out.println("End PostgreSQLJDBCDataModel+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    dataModel = new ReloadFromJDBCDataModel(jdbcdatamodel);
                    System.out.println("End ReloadFromJDBCDataModel+++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                } catch (TasteException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            // log error
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException e) {}
            }
        }
    }
}
