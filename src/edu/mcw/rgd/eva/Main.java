package edu.mcw.rgd.eva;

import org.apache.commons.collections4.CollectionUtils;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Main {

    public static void main(String[] args) throws Exception {
        FileInputStream fis = null;
        Properties p = new Properties();
        String url6, filename6, rnor6;
        String url5, filename5, rnor5;
        String VcfLinedata6 = "data/", VcfLinedata5 = "data/";
        String[] urlcuts;
        ArrayList<VcfLine> VCFdata = new ArrayList<>();
        ArrayList<VcfLine> VCFdata5 = new ArrayList<>();

        File directory = new File("data/");
        if(!directory.exists())
            directory.mkdir();

        try {
            fis = new FileInputStream("connection.properties");
            p.load(fis);
            url6 = (String) p.get("file_rat_rn6");
            urlcuts = url6.split("/"); // splitting the url by underscores to get the name
            rnor6 = urlcuts[urlcuts.length - 2];
            filename6 = urlcuts[urlcuts.length - 1]; // the last object in the array is the file name
            VcfLinedata6 += filename6.substring(0, filename6.length() - 3); // the unzipped file will be the same name, but w/o the .gz

            url5 = (String) p.get("file_rat_rn5");
            urlcuts = url5.split("/");
            rnor5 = urlcuts[urlcuts.length - 2];
            filename5 = urlcuts[urlcuts.length - 1];
            VcfLinedata5 += filename5.substring(0, filename6.length() - 3);

            downloadUsingNIO(url6,"data/"+filename6);
            decompressGzipFile("data/"+filename6,VcfLinedata6);
            extractData(VcfLinedata6, VCFdata, rnor6);

            downloadUsingNIO(url5,"data/"+filename5);
            decompressGzipFile("data/"+filename5,VcfLinedata5);
            extractData(VcfLinedata5, VCFdata5, rnor5);

            databaseSYNC(VCFdata,VCFdata5);
        }
        catch (Exception e) { e.printStackTrace(); }

    } // end of main

    /*****
     * Function was taken from   https://www.journaldev.com/924/java-download-file-url
     * Function serves to download data from a url
     *****/
    private static void downloadUsingNIO(String urlStr, String file) throws IOException {
        URL url = new URL(urlStr);
        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
        rbc.close();
    }

    /*****
     * Function was taken from   https://www.journaldev.com/966/java-gzip-example-compress-decompress-file
     * Function serves to decompress the .gz file
     *****/
    private static void decompressGzipFile(String gzipFile, String newFile) {
        try {
            FileInputStream fis = new FileInputStream(gzipFile);
            GZIPInputStream gis = new GZIPInputStream(fis);
            FileOutputStream fos = new FileOutputStream(newFile);
            byte[] buffer = new byte[1024];
            int len;
            while((len = gis.read(buffer)) != -1){
                fos.write(buffer, 0, len);
            }
            //close resources
            fos.close();
            gis.close();
        } catch (IOException e) { e.printStackTrace(); }

    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @throws Exception
     *****************************/
    public static void extractData(String fileName, ArrayList<VcfLine> VCFdata, String rnor) throws Exception {

        String[] col = null;
        try {
            File f = new File(fileName); // placeholder for the file we are looking for
            String absolute = f.getAbsolutePath(); // gets the file path to file f
            File VCFfile = new File(absolute); // finds and opens the file f
            BufferedReader br = new BufferedReader(new FileReader(VCFfile));
            String lineData; // collects the data from the file lines

            while ((lineData = br.readLine()) != null)
            {
                if (lineData.charAt(0) == '#')
                {
                    if (lineData.charAt(1) != '#') {
                        col = lineData.split("\t"); // splitting the columns into an array for storage
                        col[0] = col[0].substring(1, col[0].length()); // removing the '#' in the first string
                    }
                    continue;
                }
                VCFdata.add(new VcfLine(lineData,col)); // adds the line to the array list
            } // end while
            br.close();
            if(rnor.equals("Rnor_6.0"))
                createFile(col, VCFdata, 6);
            else
                createFile(col, VCFdata, 5);

        } catch (Exception e) { e.printStackTrace(); }

    }

    /*****************************
     * createFile - creates the tab separated file or not depending if it exists then calls writeTofile
     * @param col - the column names
     * @param data - the data from the VCF file
     * @throws Exception
     *****************************/
    public static void createFile(String[] col, ArrayList<VcfLine> data, int rnorNum) throws Exception {
        String tsvFile = "data/newEVAData" + rnorNum + ".tsv";
        File dataFile = new File(tsvFile);

        if(dataFile.createNewFile()) // the only difference is whether the file is created or not
            writeTofile(dataFile,col,data);
        else
            writeTofile(dataFile,col,data);
    }

    /*****************************
     * writeTofile - writes the corresponding data into a tab separated file
     * @param dataFile - the name of the file in the directory
     * @param col - the column names
     * @param data - the data from the VCF file
     * @throws Exception
     *****************************/
    public static void writeTofile(File dataFile, String[] col, ArrayList<VcfLine> data) throws Exception {
        try {
            String absolute = dataFile.getAbsolutePath(); // gets the file path to file dataFile
            File TSVfile = new File(absolute); // finds and opens the file dataFile into a tab file
            BufferedWriter writer = new BufferedWriter(new FileWriter(TSVfile));
            for (int i = 0; i < col.length;i++)
            {
                if(col[i].toLowerCase().equals("qual") || col[i].toLowerCase().equals("filter"))
                    continue;
                if(i==col.length-1)
                    writer.write(col[i]);
                else
                    writer.write(col[i] + "\t");
            }

            for (VcfLine aData : data) {
                writer.write("\n");
                writer.write(aData.toString());
            }
            writer.close();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    /*****************************
     * databaseSYNC - Syncs this project to the rgd database
     * @param VCFdata - the new data that was just parsed
     * @throws SQLException
     *****************************/
    public static void databaseSYNC(ArrayList<VcfLine> VCFdata, ArrayList<VcfLine> VCFdata5) throws Exception {
        Connection devDB = null;
        FileInputStream fis = null;
        Properties p = new Properties();
        try {
            fis = new FileInputStream("connection.properties");
            p.load(fis);
            String dbURL = (String) p.get("database");
            String user = (String) p.get("dbusername");
            String pass = (String) p.get("dbpassword");

            devDB = DriverManager.getConnection(dbURL, user, pass); // connects to the database with JDBC

            if(devDB != null) {
                dropAndreload(VCFdata, VCFdata5, devDB); // if there is a connection. move into this function
            }
        }
        catch (Exception e) { e.printStackTrace(); }
        finally {
            try {
                if(devDB != null && !devDB.isClosed())  // close the connection if there was a connection
                    devDB.close();
            }
            catch (SQLException e) { e.printStackTrace(); }
        }
    }

    /*****************************
     * dropAndreload - deletes the current VcfLine database and repopulates the database with the new data
     * @param VCFdata - the populated arraylist with the new VcfLine data
     * @param devDB - the connection to the database
     * @throws SQLException
     *****************************/
    public static void dropAndreload(ArrayList<VcfLine> VCFdata, ArrayList<VcfLine> VCFdata5, Connection devDB) throws SQLException {
        ArrayList<Eva> dbData = new ArrayList<>();
        ArrayList<Eva> convertedVCF = new ArrayList<>();
        try {
            grabDBdata(dbData, devDB, 0, 5452734);  // subset size from the database
//            String drop = "DELETE FROM EVA";
            Statement stmt = devDB.createStatement();
//            stmt.executeUpdate(drop); // Deletes the current table

            ResultSet rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_6.0'");
            int mapkey6 = 0, mapkey5 = 0;
            while(rs.next())    {mapkey6 = rs.getInt("MAP_KEY");}
            rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_5.0'");
            while(rs.next())    {mapkey5 = rs.getInt("MAP_KEY");}
            stmt.close();

            addMK(VCFdata,mapkey6);
            addMK(VCFdata5,mapkey5);
            convertToEva(convertedVCF,VCFdata);
            convertToEva(convertedVCF,VCFdata5);

            setOperation(convertedVCF,dbData,devDB);
        }
        catch (SQLException e) { e.printStackTrace(); }
    }

    public static void addMK(ArrayList<VcfLine> data, int key) {
        for(VcfLine d : data)
            d.setRnor(key);
    }

    public static void grabDBdata(ArrayList<Eva> dbData, Connection devDb, int subsetStart, int subsetEnd) {
        try {
            Statement select = devDb.createStatement();
            // selects the subset of rows in the data
            ResultSet evaTable = select.executeQuery("select * from (Select EVA_ID, CHROMOSOME, POS, RS_ID, REF_NUC," +
                    " VAR_NUC, SO_TERM_ACC, MAP_KEY, ROW_NUMBER() over (ORDER BY EVA_ID asc) as " +
                    "RowNo from EVA) t where RowNo between "+ subsetStart +" AND "+ subsetEnd);
            int i = subsetStart;
            while (evaTable.next() && i < subsetEnd){
                Eva line = new Eva();
                line.setEvaid(evaTable.getInt("EVA_ID"));
                line.setChromosome(evaTable.getString("CHROMOSOME"));
                line.setPos(evaTable.getInt("POS"));
                line.setRsid(evaTable.getString("RS_ID"));
                line.setRefnuc(evaTable.getString("REF_NUC"));
                line.setVarnuc(evaTable.getString("VAR_NUC"));
                line.setSoterm(evaTable.getString("SO_TERM_ACC"));
                line.setMapkey(evaTable.getInt("MAP_KEY"));
                dbData.add(line);
                i++;
                System.out.println(i);
            }
            select.close();
        }
        catch (SQLException e){e.printStackTrace();}
    }

    public static void convertToEva(ArrayList<Eva> VCFtoEva, ArrayList<VcfLine> VCFdata) {
        for(VcfLine e : VCFdata) {
            Eva temp = new Eva();
            temp.setChromosome(e.getChrom());
            temp.setPos(e.getPos());
            temp.setRsid(e.getID());
            temp.setRefnuc(e.getRef());
            temp.setVarnuc(e.getAlt());
            temp.setSoterm(e.getInfo());
            temp.setMapkey(e.getRnor());
            VCFtoEva.add(temp);
        }
    }

    /*****************************
     * reloadDB - loops through the VCFdata and uploads data to the database
     * @param tobeInserted - data to be inserted into DB
     *****************************/
    public static void reloadDB(Collection<Eva> tobeInserted, Connection devDB) {
        try {
            // reload is setting up the string to be inserted into the database
            String reload = "INSERT INTO EVA (EVA_ID, CHROMOSOME, POS, RS_ID, REF_NUC, VAR_NUC, SO_TERM_ACC, MAP_KEY)"
                    + " VALUES (EVA_SEQ.NEXTVAL,?,?,?,?,?,?,?)";
            PreparedStatement ps = devDB.prepareStatement(reload);
            int i = 0;
            final int batchSize = 1000; // size that determines when to execute
            for (Eva data : tobeInserted) { // sets the string in the order of the '?' in the reload String
                ps.setString(1, data.getChromosome());
                ps.setInt(2, data.getPos());
                ps.setString(3, data.getRsid());
                ps.setString(4, data.getRefnuc());
                ps.setString(5, data.getVarnuc());
                ps.setString(6, data.getSoterm());
                ps.setInt(7, data.getMapkey());

                ps.addBatch();
                if (++i % batchSize == 0)
                    ps.executeBatch(); // executes the batch and adds 1000 Eva objects to the database
            }
            ps.executeBatch(); // executes the remaining Eva objects in the batch
            ps.close();
        }
        catch (SQLException e){ e.printStackTrace(); }
    }

    public static void deletefromDB(Collection<Eva> tobeDeleted, Connection devDB) {
        try{
            String remove = "DELETE FROM EVA WHERE EVA_ID=?";
            PreparedStatement delete =devDB.prepareStatement(remove);
            int cnt = 0;
            final int batchsize = 1000;
            for (Eva data : tobeDeleted) {
                delete.setInt(1,data.getEvaid());
                delete.addBatch();
                if(++cnt % batchsize == 0)
                    delete.executeBatch();
            }
            delete.executeBatch();
            delete.close();
        }
        catch (SQLException e){e.printStackTrace();}
    }

    public static void setOperation(ArrayList<Eva> newData, ArrayList<Eva> oldData, Connection devDB) {
        Set<Eva> incoming = new HashSet<>(newData);
        Set<Eva> inRGD = new HashSet<>(oldData);

        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        // determines objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);

        Collection<Eva> mathcing = CollectionUtils.intersection(inRGD,incoming);

        if(!tobeInserted.isEmpty()) {
            reloadDB(tobeInserted,devDB);
            System.out.println("New Eva objects to be inserted: " + tobeInserted.size());
        }
        if(!tobeDeleted.isEmpty()) {
            deletefromDB(tobeDeleted,devDB);
            System.out.println("Old Eva objects to be deleted: " + tobeDeleted.size());
        }
        int matchingEVA = mathcing.size();
        if(matchingEVA!=0)
            System.out.println("Eva objects that match: "+mathcing.size());

    }
}