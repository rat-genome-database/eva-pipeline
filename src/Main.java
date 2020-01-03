import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.io.*;
import java.util.zip.GZIPInputStream;
import java.sql.*;
import java.util.Properties;


public class Main {

    public static void main(String[] args) throws Exception {

        String url = "ftp://ftp.ebi.ac.uk/pub/databases/eva/rs_releases/release_1/by_species/Rat_10116/Rnor_6.0/GCA_000001895.4_current_ids.vcf.gz";
        String[] urlcuts = url.split("_"); // splitting the url by underscores to get the name
        String filename = urlcuts[urlcuts.length-1]; // the last object in the array is the file name
        String EVAdata = filename.substring(0, filename.length()-3); // the unzipped file will be the same name, but w/o the .gz
        ArrayList<Eva> VCFdata = new ArrayList<>();

        try {
            downloadUsingNIO(url,"C:/Github/eva-pipeline/"+filename);
            decompressGzipFile("C:/Github/eva-pipeline/"+filename,"C:/Github/eva-pipeline/"+EVAdata);

            extractData(EVAdata, VCFdata);

            databaseSYNC(VCFdata);
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
    public static void extractData(String fileName, ArrayList<Eva> VCFdata) throws Exception {

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
                VCFdata.add(new Eva(lineData,col)); // adds the line to the array list
            } // end while
            br.close();
            createFile(col, VCFdata);
        } catch (Exception e) { e.printStackTrace(); }

    }

    /*****************************
     * createFile - creates the tab separated file or not depending if it exists then calls writeTofile
     * @param col - the column names
     * @param data - the data from the VCF file
     * @throws Exception
     *****************************/
    public static void createFile(String[] col, ArrayList<Eva> data) throws Exception {
        File dataFile = new File("EVAdata.tsv");

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
    public static void databaseSYNC(ArrayList<Eva> VCFdata) throws Exception {
        Connection devDB = null;
        try {
            FileInputStream fis = new FileInputStream("connection.properties");
            Properties p = new Properties();
            p.load(fis);

            String dbURL = (String) p.get("database");
            String user = (String) p.get("dbusername");
            String pass = (String) p.get("dbpassword");

            devDB = DriverManager.getConnection(dbURL, user, pass); // connects to the database with JDBC

            if(devDB != null)
                dropAndreload(VCFdata, devDB); // if there is a connection. move into this function
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
    public static void dropAndreload(ArrayList<Eva> VCFdata, Connection devDB) throws SQLException {
        try {
            String drop = "DELETE FROM EVA";
            Statement stmt = devDB.createStatement();
            stmt.executeUpdate(drop); // Deletes the current table
            stmt.close();
            String reload = "INSERT INTO EVA (EVA_ID, CHROMOSOME, POS, RS_ID, REF_NUC, VAR_NUC, SO_TERM_ACC) " +
                    "VALUES (?,?,?,?,?,?,?)"; // reload is setting up the string to be inserted into the database
            PreparedStatement ps = devDB.prepareStatement(reload);
            final int batchSize = 2000; // size that determines when to execute
            int i = 0;
            for (Eva data : VCFdata)
            {  // sets the string in the order of the '?' in the insert String
                ps.setInt(1, i+1);
                ps.setString(2, data.chrom);
                ps.setInt(3, data.pos);
                ps.setString(4, data.ID);
                ps.setString(5, data.ref);
                ps.setString(6, data.alt);
                ps.setString(7, data.info);
                ps.addBatch();

                if(++i % batchSize == 0)
                    ps.executeBatch(); // executes the batch and adds 2000 Eva's to the database
            }
            ps.executeBatch(); // executes the remaining Eva's in the batch
            ps.close();
        }
        catch (SQLException e) { e.printStackTrace(); }
    }
}

class Eva {
    public String chrom  = null;
    public int    pos;
    public String ID     = null;
    public String ref    = null;
    public String alt    = null;
    public String qual   = null;
    public String filter = null;
    public String info   = null;

    /*****************************
     * Eva: Constructor
     * Constructor serves to split the data into each field
     * returns a new Eva object with the data stored
     *****************************/
    public Eva(String data , String[] col ) {
        String[] myData = data.split("\t");
        for(int i = 0;i<col.length;i++)
        {
            if (col[i].toLowerCase().equals("chrom"))
                this.chrom = myData[i];
            else if (col[i].toLowerCase().equals("pos"))
                this.pos = Integer.parseInt(myData[i]);
            else if (col[i].toLowerCase().equals("id"))
                this.ID = myData[i];
            else if (col[i].toLowerCase().equals("ref"))
                this.ref = myData[i];
            else if (col[i].toLowerCase().equals("alt"))
                this.alt = myData[i];
            else if (col[i].toLowerCase().equals("qual"))
                this.qual = myData[i];
            else if (col[i].toLowerCase().equals("filter"))
                this.filter = myData[i];
            else if (col[i].toLowerCase().equals("info"))
            {
                String[] Info = myData[i].split("SO:"); // was "VC="
                if(Info.length >= 2)
                    this.info = Info[Info.length-1];
            }
        }
    }
    @Override
    public String toString() { return chrom + "\t" + pos + "\t"+ ID + "\t" + ref + "\t" + alt + "\t" + info; }
}