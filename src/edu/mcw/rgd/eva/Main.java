package edu.mcw.rgd.eva;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class Main {

    private String version;
    private Map<Integer, String> incomingFiles;
    private int subetStart;
    private int subetEnd;

    protected Logger logger = Logger.getLogger("status");

    private ArrayList<VcfLine> VCFdata = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.eva.Main mainBean = (edu.mcw.rgd.eva.Main) (bf.getBean("main"));
        try{
            mainBean.run();
        }catch (Exception e) {e.printStackTrace();}
    } // end of main

    public void run(){

        Set<Integer> keys = getIncomingFiles().keySet();
        Integer[] IFkeys = keys.toArray(new Integer[getIncomingFiles().size()]);

        File directory = new File("data/");
        if(!directory.exists())
            directory.mkdir();

        try {
            for(Integer i : IFkeys) {
                String[] urlcuts;
                String url, filename, type, VCFlinedata = "data/";
                url = getIncomingFiles().get(i);
                urlcuts = url.split("/");
                filename = urlcuts[urlcuts.length - 1];
                type = urlcuts[urlcuts.length - 2];
                VCFlinedata += filename.substring(0, filename.length() - 3);
                downloadUsingNIO(url,"data/"+filename);
                decompressGzipFile("data/"+filename, VCFlinedata);
                extractData(VCFlinedata, VCFdata, type, i);
            }
            databaseSYNC();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @throws Exception
     *****************************/
    public void extractData(String fileName, ArrayList<VcfLine> VCFdata, String rnor, int key) throws Exception {

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
                VCFdata.add(new VcfLine(lineData,col, key)); // adds the line to the array list
            } // end while
            br.close();
            createFile(col, VCFdata, rnor);
        } catch (Exception e) { e.printStackTrace(); }

    }

    /*****************************
     * createFile - creates the tab separated file or not depending if it exists then calls writeTofile
     * @param col - the column names
     * @param data - the data from the VCF file
     * @throws Exception
     *****************************/
    public void createFile(String[] col, ArrayList<VcfLine> data, String rnor) throws Exception {
        String tsvFile = "data/newEVAData-" + rnor + ".tsv";
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
    public void writeTofile(File dataFile, String[] col, ArrayList<VcfLine> data) throws Exception {
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
     * @throws Exception
     *****************************/
    public void databaseSYNC() throws Exception {
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
                updateDB(devDB); // if there is a connection. move into this function
            }
        }
        catch (Exception e) {
            e.printStackTrace(); }
        finally {
            try {
                if(devDB != null && !devDB.isClosed())  // close the connection if there was a connection
                    devDB.close();
            }
            catch (SQLException e) { e.printStackTrace(); }
        }
    }

    /*****************************
     * updateDB - deletes the current VcfLine database and repopulates the database with the new data
     * @param devDB - the connection to the database
     *****************************/
    public void updateDB(Connection devDB) {
        ArrayList<Eva> dbData = new ArrayList<>();
        ArrayList<Eva> convertedVCF = new ArrayList<>();
        try {
            int subStart = getSubetStart(), subEnd = getSubetEnd();
            if((subEnd-subStart) <= 0) {
                logger.warning("Start and End values are the same or the endVal < startVal... exiting");
                return;
            }
            grabDBdata(dbData, devDB, subStart, subEnd);  // subset size from the database

/*            Statement stmt = devDB.createStatement();
            ResultSet rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_6.0'");
            int mapkey6 = 0, mapkey5 = 0;
            while(rs.next())    {mapkey6 = rs.getInt("MAP_KEY");}
            rs = stmt.executeQuery("Select MAP_key from maps where map_name='Rnor_5.0'");
            while(rs.next())    {mapkey5 = rs.getInt("MAP_KEY");}
            stmt.close();*/
            convertToEva(convertedVCF,VCFdata);

            setOperation(convertedVCF,dbData,devDB);
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    public void addMK(ArrayList<VcfLine> data, int key) {
        for(VcfLine d : data)
            d.setMapkey(key);
    }

    public void grabDBdata(ArrayList<Eva> dbData, Connection devDb, int subsetStart, int subsetEnd) {
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
//                System.out.println(i);
            }
            select.close();
        }
        catch (SQLException e){e.printStackTrace();}
    }

    public void convertToEva(ArrayList<Eva> VCFtoEva, ArrayList<VcfLine> VCFdata) {
        for(VcfLine e : VCFdata) {
            Eva temp = new Eva();
            temp.setChromosome(e.getChrom());
            temp.setPos(e.getPos());
            temp.setRsid(e.getID());
            temp.setRefnuc(e.getRef());
            temp.setVarnuc(e.getAlt());
            temp.setSoterm(e.getInfo());
            temp.setMapkey(e.getMapkey());
            VCFtoEva.add(temp);
        }
    }

    /*****************************
     * reloadDB - loops through the VCFdata and uploads data to the database
     * @param tobeInserted - data to be inserted into DB
     *****************************/
    public void reloadDB(Collection<Eva> tobeInserted, Connection devDB) {
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

    public void deletefromDB(Collection<Eva> tobeDeleted, Connection devDB) {
        try{
            String remove = "DELETE FROM EVA WHERE EVA_ID=?";
            PreparedStatement delete = devDB.prepareStatement(remove);
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

    public void setOperation(ArrayList<Eva> newData, ArrayList<Eva> oldData, Connection devDB) {
        Set<Eva> incoming = new HashSet<>(newData);
        Set<Eva> inRGD = new HashSet<>(oldData);

        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        // determines old objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);

        Collection<Eva> mathcing = CollectionUtils.intersection(inRGD,incoming);

        if(!tobeInserted.isEmpty()) {
            reloadDB(tobeInserted,devDB);
           logger.info("New Eva objects to be inserted: " + tobeInserted.size());
        }
        if(!tobeDeleted.isEmpty()) {
            deletefromDB(tobeDeleted,devDB);
            logger.info("Old Eva objects to be deleted: " + tobeDeleted.size());
        }
        int matchingEVA = mathcing.size();
        if(matchingEVA!=0)
            logger.info("Eva objects that match: "+mathcing.size());

    }

    /*****
     * Function was taken from   https://www.journaldev.com/924/java-download-file-url
     * Function serves to download data from a url
     *****/
    private void downloadUsingNIO(String urlStr, String file) throws IOException {
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
    private void decompressGzipFile(String gzipFile, String newFile) {
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

    public void setVersion(String version) { this.version = version; }

    public String getVersion() { return version; }

    public void setIncomingFiles(Map<Integer, String> incomingFiles) { this.incomingFiles = incomingFiles; }

    public Map<Integer, String> getIncomingFiles() {return incomingFiles;}

    public void setSubetStart(int subetStart) { this.subetStart = subetStart; }

    public int getSubetStart() { return subetStart; }

    public void setSubetEnd(int subetEnd) { this.subetEnd = subetEnd; }

    public int getSubetEnd() { return subetEnd; }
}