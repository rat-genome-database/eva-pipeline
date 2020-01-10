package edu.mcw.rgd.eva;


import org.apache.commons.collections4.CollectionUtils;
import sun.awt.image.ImageWatched;


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
        String EVAdata6 = "data/", EVAdata5 = "data/";
        String[] urlcuts;
        ArrayList<Eva> VCFdata = new ArrayList<>();
        ArrayList<Eva> VCFdata5 = new ArrayList<>();

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
            EVAdata6 += filename6.substring(0, filename6.length() - 3); // the unzipped file will be the same name, but w/o the .gz

            url5 = (String) p.get("file_rat_rn5");
            urlcuts = url5.split("/");
            rnor5 = urlcuts[urlcuts.length - 2];
            filename5 = urlcuts[urlcuts.length - 1];
            EVAdata5 += filename5.substring(0, filename6.length() - 3);

            downloadUsingNIO(url6,"data/"+filename6);
            decompressGzipFile("data/"+filename6,EVAdata6);
            extractData(EVAdata6, VCFdata, rnor6);

            downloadUsingNIO(url5,"data/"+filename5);
            decompressGzipFile("data/"+filename5,EVAdata5);
            extractData(EVAdata5, VCFdata5, rnor5);

            setTheory(VCFdata, VCFdata5);

            //databaseSYNC(VCFdata,VCFdata5);
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
    public static void extractData(String fileName, ArrayList<Eva> VCFdata, String rnor) throws Exception {

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
                VCFdata.add(new Eva(lineData,col, rnor)); // adds the line to the array list
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
    public static void createFile(String[] col, ArrayList<Eva> data, int rnorNum) throws Exception {
        String tsvFile = "data/EVAdata" + rnorNum + ".tsv";
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
    public static void writeTofile(File dataFile, String[] col, ArrayList<Eva> data) throws Exception {
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

            for (Eva aData : data) {
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
    public static void databaseSYNC(ArrayList<Eva> VCFdata, ArrayList<Eva> VCFdata5) throws Exception {
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

            if(devDB != null)
                dropAndreload(VCFdata, VCFdata5, devDB); // if there is a connection. move into this function
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
     * dropAndreload - deletes the current Eva database and repopulates the database with the new data
     * @param VCFdata - the populated arraylist with the new EVA data
     * @param devDB - the connection to the database
     * @throws SQLException
     *****************************/
    public static void dropAndreload(ArrayList<Eva> VCFdata, ArrayList<Eva> VCFdata5, Connection devDB) throws SQLException {
        try {
            String drop = "DELETE FROM EVA";
            Statement stmt = devDB.createStatement();
            stmt.executeUpdate(drop); // Deletes the current table

            ResultSet rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_6.0'"); // maybe change in case name "Rnor_6.0" changes
            int mapkey6 = 0, mapkey5 = 0;
            while(rs.next())
               mapkey6 = rs.getInt("MAP_KEY");
            rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_5.0'");
            while(rs.next())
                mapkey5 = rs.getInt("MAP_KEY");
            stmt.close();

            String reload = "INSERT INTO EVA (EVA_ID, CHROMOSOME, POS, RS_ID, REF_NUC, VAR_NUC, SO_TERM_ACC, MAP_KEY) "
                    + "VALUES (?,?,?,?,?,?,?,?)"; // reload is setting up the string to be inserted into the database
            PreparedStatement ps = devDB.prepareStatement(reload);
            int i = 0;
            reloadDB(VCFdata,ps, mapkey6, i);
            ps.clearBatch();
            i = VCFdata.size();
            reloadDB(VCFdata5,ps,mapkey5, i);

            ps.close();
        }
        catch (SQLException e) { e.printStackTrace(); }
    }

    /*****************************
     * reloadDB - loops through the VCFdata and uploads data to the database
     * @param VCFdata - Eva data
     * @param ps - the prepared statement that will be modified to insert into database
     * @param rnor - map key for the data type
     * @param i - value for the EVA_ID
     *****************************/
    public static void reloadDB(ArrayList<Eva> VCFdata, PreparedStatement ps, int rnor, int i) {
        try {
            final int batchSize = 2000; // size that determines when to execute

            for (Eva data : VCFdata) {  // sets the string in the order of the '?' in the insert String
                ps.setInt(1, i + 1);
                ps.setString(2, data.getChrom());
                ps.setInt(3, data.getPos());
                ps.setString(4, data.getID());
                ps.setString(5, data.getRef());
                ps.setString(6, data.getAlt());
                ps.setString(7, data.getInfo());
                ps.setInt(8, rnor);
                /*if(data.getRnor().equals("Rnor_6.0"))
                    ps.setInt(8, rnor);
                else
                    ps.setInt(8, )*/
                ps.addBatch();

                if (++i % batchSize == 0)
                    ps.executeBatch(); // executes the batch and adds 2000 Eva's to the database
            }
            ps.executeBatch(); // executes the remaining Eva's in the batch
        }
        catch (SQLException e){ e.printStackTrace(); }
    }

    // Name subject to change
    public static void setTheory(ArrayList<Eva> data6, ArrayList<Eva> data5) {
        LinkedHashSet<Eva> d6subd5 = new LinkedHashSet<>();
        LinkedHashSet<Eva> d5subd6 = new LinkedHashSet<>();
        LinkedHashSet<Eva> eva6 = new LinkedHashSet<>(data6);
        LinkedHashSet<Eva> eva5 = new LinkedHashSet<>(data5);
        LinkedHashSet<Eva> intersect = new LinkedHashSet<>();
        LinkedHashSet<Eva> noInter = new LinkedHashSet<>();

        // A - B
        for (Eva e : eva6)
            if (!eva5.contains(e)) // adds objects that is not in the other set
                d6subd5.add(e);

        // B - A
        for (Eva e : eva5)
            if (!eva6.contains(e)) // adds objects that is not in the other set
                d5subd6.add(e);
        System.out.println("Objects not in List 2: " + d6subd5.size() + ". Objects not in List 1: " + d5subd6.size());
        System.out.println("A - B = " + d6subd5.size() + " and AL size is " + data6.size()
                + ". B - A  = " + d5subd6.size() + " and AL size is " + data5.size());

        // A ∩ B
        for (Eva e : eva5)
            if (eva6.contains(e)) // adds iff the object is in the other set
                intersect.add(e);

        if (intersect.isEmpty())
            System.out.println("Lists are disjoint");
        else
            System.out.println("Intersection is " + intersect);

        // ~(A ∩ B) = ~A U ~B = S - (A ∩ B) ≈ (A U B) - (A ∩ B)
        for (Eva e : eva6)
            if (!intersect.contains(e))
                noInter.add(e);
        for (Eva e : eva5)
            if (!intersect.contains(e))
                noInter.add(e);

        System.out.println("Amount of objects but the intersection " + noInter.size());
    }
}