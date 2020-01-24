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
    private int subsetStart;
    private int subsetEnd;

    protected Logger logger = Logger.getLogger("status");

    private ArrayList<VcfLine> VCFdata = new ArrayList<>();


    public static void main(String[] args) {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        edu.mcw.rgd.eva.Main mainBean = (edu.mcw.rgd.eva.Main) (bf.getBean("main"));
        try{
            mainBean.run();
        }catch (Exception e) {e.printStackTrace();}
    } // end of main

    public void run(){

        Set<Integer> mapKeys = getIncomingFiles().keySet();
        File directory = new File("data/");
        if(!directory.exists())
            directory.mkdir();

        try {
            for(Integer mapKey : mapKeys) {
                String[] urlcuts;
                String url, filename, type, VCFlinedata = "data/";
                url = getIncomingFiles().get(mapKey);
                urlcuts = url.split("/");
                filename = urlcuts[urlcuts.length - 1];
                type = urlcuts[urlcuts.length - 2];
                VCFlinedata += filename.substring(0, filename.length() - 3);
                downloadUsingNIO(url,"data/"+filename);
                decompressGzipFile("data/"+filename, VCFlinedata);
                extractData(VCFlinedata, VCFdata, type, mapKey);
            }
            updateDB();
        }
        catch (Exception e) { e.printStackTrace(); }
    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     *****************************/
    public void extractData(String fileName, ArrayList<VcfLine> VCFdata, String rnor, int key) {

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
     *****************************/
    public void writeTofile(File dataFile, String[] col, ArrayList<VcfLine> data)  {
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
     * updateDB - converts the VCFdata to Eva objects then does set operations
     *****************************/
    public void updateDB() throws Exception {
//        ArrayList<Eva> dbData = new ArrayList<>();
//        grabDBdata(dbData, devDB, subStart, subEnd);  // subset size from the database
        ArrayList<Eva> incomingData = new ArrayList<>();
        convertToEva(incomingData,VCFdata);
        setOperation(incomingData);
    }

    /*****************************
     * setOperation - compares the data in the database with the new data
     * @param incoming - incoming data to be compared
     * @throws Exception
     *****************************/
    public void setOperation(ArrayList<Eva> incoming) throws Exception{
        EvaDAO dao = new EvaDAO();
        int subStart = getSubsetStart(), subEnd = getSubsetEnd();
        List<Eva> inDB = dao.getActiveArrayIdEvaFromEnsembl(subStart,subEnd);

        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inDB);
        // determines old objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inDB, incoming);

        Collection<Eva> mathcing = CollectionUtils.intersection(inDB,incoming);

        if(!tobeInserted.isEmpty()) {
            List<Eva> insertMe = new ArrayList<>(tobeInserted);
            dao.insertEva(insertMe);
            logger.info("New Eva objects to be inserted: " + tobeInserted.size());
        }
        if(!tobeDeleted.isEmpty()) {
            for(Eva e : tobeDeleted)
                dao.deleteEva(e.getEvaid());
            logger.info("Old Eva objects to be deleted: " + tobeDeleted.size());
        }
        int matchingEVA = mathcing.size();
        if(matchingEVA!=0)
            logger.info("Eva objects that match: "+mathcing.size());

    }

    /*****
     * Function was taken from   https://www.journaldev.com/924/java-download-file-url
     * Function serves to download data from a given url
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



    public void databaseSYNC() throws Exception {
        Connection devDB = null;
        Properties p = new Properties();
        try {
            FileInputStream fis = new FileInputStream("connection.properties");
            p.load(fis);
            String dbURL = (String) p.get("database");
            String user = (String) p.get("dbusername");
            String pass = (String) p.get("dbpassword");

            devDB = DriverManager.getConnection(dbURL, user, pass); // connects to the database with JDBC

            if(devDB != null) {
                updateDB();//(devDB); // if there is a connection. move into this function
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



    public void setVersion(String version) { this.version = version; }

    public String getVersion() { return version; }

    public void setIncomingFiles(Map<Integer, String> incomingFiles) { this.incomingFiles = incomingFiles; }

    public Map<Integer, String> getIncomingFiles() {return incomingFiles;}

    public void setSubsetStart(int subsetStart) {
        this.subsetStart = subsetStart;
    }

    public int getSubsetStart() {
        return subsetStart;
    }

    public void setSubsetEnd(int subsetEnd) {
        this.subsetEnd = subsetEnd;
    }

    public int getSubsetEnd() {
        return subsetEnd;
    }
}