package edu.mcw.rgd.eva;

public class VcfLine {
    private String chrom  = null;
    private int    pos;
    private String ID     = null;
    private String ref    = null;
    private String alt    = null;
    private String qual   = null;
    private String filter = null;
    private String info   = null;
    private String SID = null;
    private int mapKey;

    public String getChrom(){return chrom;}
    public int getPos()     {return pos;}
    public String getID()   {return ID;}
    public String getRef()  {return ref;}
    public String getAlt()  {return alt;}
    public String getInfo() {return info;}
    public int getMapKey() {return mapKey;}

    public void setMapkey(int mapKey) {this.mapKey = mapKey;}

    /*****************************
     * VcfLine: Constructor
     * Constructor serves to split the data into each field
     * returns a new VcfLine object with the data stored
     *****************************/
    public VcfLine(String data , String[] col, int key) throws Exception {
        String[] myData = data.split("\t");
        mapKey = key;
        for(int i = 0; i<col.length; i++) {
            switch (col[i].toUpperCase()) {
                case "CHROM":
                    if (myData[i].toLowerCase().startsWith("chr")) {
                        String chromNum = myData[i].substring(3); // removes the chr
                        try {
                            this.chrom = Integer.valueOf(chromNum).toString();
                        } // String is an int
                        catch (Exception ignore) {
                            this.chrom = chromNum;
                        }
                    } else if (myData[i].length() == 4 || myData[i].length() == 5) {
                        String chrom = myData[i].substring(3);
                        this.chrom = chrom; // does not have chr prefix
                    } else if (myData[i].length() < 3) {
                        String chromNum = myData[i];
                        try {
                            this.chrom = Integer.valueOf(chromNum).toString();
                        } // String is an int
                        catch (Exception ignore) {
                            this.chrom = chromNum;
                        } // x or y
                    } else
                        throw new Exception("NEW CHROMOSOME CASE! " + myData[i]);
                    break;
                case "POS":
                    this.pos = Integer.parseInt(myData[i]);
                    break;
                case "ID":
                    this.ID = myData[i];
                    break;
                case "REF":
                    this.ref = myData[i];
                    break;
                case "ALT":
                    this.alt = myData[i];
                    break;
                case "QUAL":
                    this.qual = myData[i];
                    break;
                case "FILTER":
                    this.filter = myData[i];
                    break;
                case "INFO":
                    String[] study = myData[i].split("SID=");
                    String[] fullSID = study[study.length - 1].split(";");
                    SID = fullSID[0];
                    String[] Info = myData[i].split("VC="); // was "SO:"

                    if (Info.length >= 2)
                        this.info = Info[Info.length - 1];
                    break;
            }
        }
    }

    @Override
    public String toString() { return chrom+"\t"+pos+"\t"+ID+"\t"+ref+"\t"+alt+"\t"+info; }
}