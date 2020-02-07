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
    public VcfLine(String data , String[] col, int key) {
        String[] myData = data.split("\t");
        mapKey = key;

        for(int i = 0; i<col.length; i++) {
            if (col[i].equals("CHROM")) {
                if(myData[i].length() > 3) {
                    String chromnum = myData[i].substring(3);
                    this.chrom = chromnum;
                }
                else
                    this.chrom=myData[i];
            }
            else if (col[i].equals("POS"))
                this.pos = Integer.parseInt(myData[i]);
            else if (col[i].equals("ID"))
                this.ID = myData[i];
            else if (col[i].equals("REF"))
                this.ref = myData[i];
            else if (col[i].equals("ALT"))
                this.alt = myData[i];
            else if (col[i].equals("QUAL"))
                this.qual = myData[i];
            else if (col[i].equals("FILTER"))
                this.filter = myData[i];
            else if (col[i].equals("INFO")) {
                String[] Info = myData[i].split("SO:"); // was "VC="
                if(Info.length >= 2)
                    this.info = Info[Info.length-1];
            }
        }
    }
    @Override
    public String toString() { return chrom+"\t"+pos+"\t"+ID+"\t"+ref+"\t"+alt+"\t"+info; }
}
