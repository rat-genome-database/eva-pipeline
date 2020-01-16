package edu.mcw.rgd.eva;

import edu.mcw.rgd.process.Utils;

public class Eva {
    private int evaid;
    private String chromosome;
    private int pos;
    private String rsid;
    private String refnuc;
    private String varnuc;
    private String soterm;
    private int mapkey;

    public int getEvaid(){ return evaid; }
    public String getChromosome(){ return chromosome; }
    public int getPos(){ return pos; }
    public String getRsid(){ return rsid; }
    public String getRefnuc(){ return refnuc; }
    public String getVarnuc(){ return varnuc; }
    public String getSoterm(){ return soterm; }
    public int getMapkey(){ return mapkey; }

    public void setEvaid(int evaid) {this.evaid = evaid;}
    public void setChromosome(String chromosome) {this.chromosome = chromosome;}
    public void setPos(int pos) {this.pos = pos;}
    public void setRsid(String rsid) {this.rsid = rsid;}
    public void setRefnuc(String refnuc) {this.refnuc = refnuc;}
    public void setVarnuc(String varnuc) {this.varnuc = varnuc;}
    public void setSoterm(String soterm) {this.soterm = soterm;}
    public void setMapkey(int mapkey) {this.mapkey = mapkey;}

    @Override
    public boolean equals(Object obj) {
        Eva e = (Eva)obj;
        return Utils.stringsAreEqual(chromosome, e.getChromosome()) && pos==e.getPos()
                && Utils.stringsAreEqual(rsid,e.getRsid()) && Utils.stringsAreEqual(refnuc,e.getRefnuc())
                && Utils.stringsAreEqual(varnuc, e.getVarnuc());
    }

    @Override
    public int hashCode() {
        return getPos() ^ Utils.defaultString(chromosome).hashCode() ^ Utils.defaultString(rsid).hashCode()
                ^ Utils.defaultString(refnuc).hashCode() ^ Utils.defaultString(varnuc).hashCode();
    }
}
