package edu.mcw.rgd.eva;

import java.util.List;
import java.util.Map;

/**
 * Created by llamers on 3/27/2020.
 */
public class EvaAPI {
    private String evaName; // RS_ID
    private String source; // aka BUILD
    private int mapKey;
    private String chromosome;
    private int position;
    private String snpClass; // variant type
    private String molType;
    private String genotype;
    private Double avgHetroScore;
    private Double stdError;
    private String hetroType;
    private String allele;
    private Double mafFrequency;
    private Integer mafSampleSize;
    private String mafAllele; // VAR_NUC
    private String functionClass;
    private int mapLocCount;
    private String ancestralAllele;
    private String clinicalSignificance;
    private String refAllele; // REF_NUC
    private List<String> hgvs;
    private Map<String, Integer> mergeHistory;


    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getSnpClass() {
        return snpClass;
    }

    public void setSnpClass(String snpClass) {
        this.snpClass = snpClass;
    }

    public Double getAvgHetroScore() {
        return avgHetroScore;
    }

    public void setAvgHetroScore(Double avgHetroScore) {
        this.avgHetroScore = avgHetroScore;
    }

    public String getGenotype() {
        return genotype;
    }

    public void setGenotype(String genotype) {
        this.genotype = genotype;
    }

    public Double getStdError() {
        return stdError;
    }

    public void setStdError(Double stdError) {
        this.stdError = stdError;
    }

    public String getAllele() {
        return allele;
    }

    public void setAllele(String allele) {
        this.allele = allele;
    }

    public Double getMafFrequency() {
        return mafFrequency;
    }

    public void setMafFrequency(Double mafFrequency) {
        this.mafFrequency = mafFrequency;
    }

    public Integer getMafSampleSize() {
        return mafSampleSize;
    }

    public void setMafSampleSize(Integer mafSampleSize) {
        this.mafSampleSize = mafSampleSize;
    }

    public String getHetroType() {
        return hetroType;
    }

    public void setHetroType(String hetroType) {
        this.hetroType = hetroType;
    }

    public String getMolType() {
        return molType;
    }

    public void setMolType(String molType) {
        this.molType = molType;
    }

    public String getMafAllele() {
        return mafAllele;
    }

    public void setMafAllele(String mafAllele) {
        this.mafAllele = mafAllele;
    }

    public String getFunctionClass() {
        return functionClass;
    }

    public void setFunctionClass(String functionClass) {
        this.functionClass = functionClass;
    }

    public int getMapLocCount() {
        return mapLocCount;
    }

    public void setMapLocCount(int mapLocCount) {
        this.mapLocCount = mapLocCount;
    }

    public String getAncestralAllele() {
        return ancestralAllele;
    }

    public void setAncestralAllele(String ancestralAllele) {
        this.ancestralAllele = ancestralAllele;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public void setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
    }

    public String getRefAllele() {
        return refAllele;
    }

    public void setRefAllele(String refAllele) {
        this.refAllele = refAllele;
    }

    public List<String> getHgvs() {
        return hgvs;
    }

    public void setHgvs(List<String> hgvs) {
        this.hgvs = hgvs;
    }

    public Map<String, Integer> getMergeHistory() {
        return mergeHistory;
    }

    public void setMergeHistory(Map<String, Integer> mergeHistory) {
        this.mergeHistory = mergeHistory;
    }

    public String getEvaName() {
        return evaName;
    }

    public void setEvaName(String evaName) {
        this.evaName = evaName;
    }
}
