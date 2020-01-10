package edu.mcw.rgd.eva;

import edu.mcw.rgd.process.Utils;

import javax.rmi.CORBA.Util;

public class Eva {
    private String chrom  = null;
    private int    pos;
    private String ID     = null;
    private String ref    = null;
    private String alt    = null;
    private String qual   = null;
    private String filter = null;
    private String info   = null;
    private String rnor;

    public String getChrom(){return chrom;}
    public int getPos()     {return pos;}
    public String getID()   {return ID;}
    public String getRef()  {return ref;}
    public String getAlt()  {return alt;}
    public String getInfo() {return info;}
    public String getRnor() {return rnor;}

    public Eva()
    {
        chrom = "1";
        pos = 0;
        ID = "DEFAULT";
        ref = "A";
        alt = "G";
        info = "0001483";
    }
    /*****************************
     * Eva: Constructor
     * Constructor serves to split the data into each field
     * returns a new Eva object with the data stored
     *****************************/
    public Eva(String data , String[] col, String rnor ) {
        String[] myData = data.split("\t");
        this.rnor = rnor;
        for(int i = 0; i<col.length; i++)
        {
            if (col[i].toLowerCase().equals("chrom")) {
                if(myData[i].length() > 3)
                {
                    String chromnum = myData[i].substring(3);
                    this.chrom = chromnum;
                }
                else
                    this.chrom=myData[i];
            }
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
    public boolean equals(Object obj) {
        Eva e = (Eva)obj;
        return Utils.stringsAreEqual(chrom, e.getChrom()) && pos==e.getPos()
                && Utils.stringsAreEqual(ID,e.getID()) && Utils.stringsAreEqual(ref,e.getRef())
                && Utils.stringsAreEqual(alt, e.getAlt());
    }

    @Override
    public int hashCode() {
        return getPos() ^ Utils.defaultString(chrom).hashCode() ^ Utils.defaultString(ID).hashCode()
                ^ Utils.defaultString(ref).hashCode() ^ Utils.defaultString(alt).hashCode();
    }

    @Override
    public String toString() { return chrom+"\t"+pos+"\t"+ID+"\t"+ref+"\t"+alt+"\t"+info; }
}
