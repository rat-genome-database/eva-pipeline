import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.io.*;
import java.util.zip.GZIPInputStream;


public class Main {

    public static void main(String[] args) throws Exception {

        String url = "ftp://ftp.ebi.ac.uk/pub/databases/eva/rs_releases/release_1/by_species/Rat_10116/Rnor_6.0/GCA_000001895.4_current_ids.vcf.gz";
        String[] urlcuts = url.split("_"); // splitting the url by underscores to get the name
        String filename = urlcuts[urlcuts.length-1]; // the last object in the array is the file name
        try{
            downloadUsingNIO(url,"C:/Github/"+filename);
            String EVAdata = filename.substring(0,filename.length()-3); // the unzipped file will be the same name, but w/o the .gz

            decompressGzipFile("C:/Github/"+filename,"C:/Github/eva-pipeline/"+EVAdata);

            extractData(EVAdata);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

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
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*****************************
     * extractData serves to grab the data from the VCF file and put it into a class for storage
     * @param fileName - holds the file name of the decompressed gz file
     * @throws Exception
     *****************************/
    public static void extractData(String fileName) throws Exception {
        ArrayList<fileData> VCFdata = new ArrayList<>();
        String[] col = null;
        try {
            File f = new File(fileName); // placeholder for the file we are looking for
            String absolute = f.getAbsolutePath(); // gets the file path to file f
            File VCFfile = new File(absolute); // finds and opens the file f
            BufferedReader br = new BufferedReader(new FileReader(VCFfile));
            String lineData; // collects the data from the file lines

            while ((lineData = br.readLine()) != null)  // goes until the last line is null/nonexistent
            {
                if (lineData.charAt(0) == '#') // if the line starts with #, skip it
                {
                    if (lineData.charAt(1) != '#') {
                        col = lineData.split("\t"); // splitting the columns into an array
                        col[0] = col[0].substring(1, col[0].length()); // removing the '#' in the first string
                    }
                    continue;
                }

                VCFdata.add(new fileData(lineData,col)); // adds the line to the array list

            } // end while

            br.close();
            createFile(col, VCFdata);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*****************************
     * createFile - creates the tab separated file or not depending if it exists then calls writeTofile
     * @param col - the column names
     * @param data - the data from the VCF file
     * @throws Exception
     *****************************/
    public static void createFile(String[] col, ArrayList<fileData> data) throws Exception {
        File dataFile = new File("EVAdata.tsv");

        if(dataFile.createNewFile())
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
    public static void writeTofile(File dataFile, String[] col, ArrayList<fileData> data) throws Exception {
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

            for (fileData aData : data) {
                writer.write("\n");
                writer.write(aData.toString());
            }
            writer.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class fileData {
    public String chrom = ".";
    public String pos   = ".";
    public String ID    = ".";
    public String ref   = ".";
    public String alternate = ".";
    public String qual  = ".";
    public String filter    = ".";
    public String info  = ".";

    /************************
     * fileData: Constructor
     * Constructor serves to split the data into each field
     * returns a new fileData object with the data stored
     ************************/
    public fileData(String data , String[] col ) {
        String[] myData = data.split("\t");
        for(int i = 0;i<col.length;i++)
        {
            if (col[i].toLowerCase().equals("chrom"))
                this.chrom = myData[i];
            else if (col[i].toLowerCase().equals("pos"))
                this.pos = myData[i];
            else if (col[i].toLowerCase().equals("id"))
                this.ID = myData[i];
            else if (col[i].toLowerCase().equals("ref"))
                this.ref = myData[i];
            else if (col[i].toLowerCase().equals("alt"))
                this.alternate = myData[i];
            else if (col[i].toLowerCase().equals("qual"))
                this.qual = myData[i];
            else if (col[i].toLowerCase().equals("filter"))
                this.filter = myData[i];
            else if (col[i].toLowerCase().equals("info"))
                this.info = myData[i];
        }

    }

    @Override
    public String toString() { return chrom + "\t" + pos + "\t"+ ID + "\t" + ref + "\t" + alternate + "\t" + info; }
}