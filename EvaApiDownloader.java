package edu.mcw.rgd.eva;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.util.Map;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.mcw.rgd.process.FileDownloader;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by llamers on 3/25/2020.
 */
public class EvaApiDownloader {

    private Map groupLabels;

    private DAO dao = new DAO();
    protected final Log logger = LogFactory.getLog("APIdump");
    private String url;

    static int rowsInserted = 0;
    static ArrayList<Eva> sendTo = new ArrayList<>();

    public EvaApiDownloader() {}

    public void downloadAllFiles() throws Exception{

        BufferedWriter dump = createGZip("logs/load.log");
        dump.write("RS ID\tChr\tPosition\tsnpClass\tgenotype\tevaBuild\tallele\tMafFreq\tMafSampleSize\tMafAllele\trefAllele\n");

        int mapKey = 360;

        List<String> chrFiles = downloadAllChromosomes(mapKey, dump);

        for (String fname : chrFiles)
        {
            List<EvaAPI> evaList = new ArrayList<>();
            processFile(fname, mapKey, dump, evaList);
            String chrom = evaList.get(0).getChromosome();
            dao.convertAPIToEva(sendTo, evaList);
            insertAndDeleteEvaObjectsByKeyAndChromosome(sendTo,mapKey,chrom);

        }

        dump.close();
    }

    BufferedWriter createGZip(String fName) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fName))));

    }

    public List<String> downloadAllChromosomes(int mapKey , BufferedWriter dump) throws Exception{
        List<String> chrFiles = new ArrayList<String>();
        String dir = "data/";

        edu.mcw.rgd.datamodel.Map assembly = MapManager.getInstance().getMap(mapKey);
        String assemblyName = assembly.getName();
        String fileOutput = dir+assemblyName+"_"+"chr#CHR#.json";

        FileDownloader fd = new FileDownloader();

        final int CHUNK_SIZE = 10000;
        java.util.Map<String,Integer> chromosomeSizes = dao.getChromosomeSizes(mapKey);
        List<String> chromosomes = new ArrayList<>(chromosomeSizes.keySet());

        String chr = "1";//chromosomes.get(0);

        //Collections.shuffle(chromosomes);
       // chromosomes.parallelStream().forEach(chr -> {
            try{
                File directory = new File("tmp/z/"+chr);
                if (! directory.exists()){
                    directory.mkdir();
                }
                int fileNr = 0;
                int chrLen = chromosomeSizes.get(chr)+10000;
                String msg = "processing chromosome "+chr+" of size "+Utils.formatThousands(chrLen)+"\n";
                logger.info(msg);
                dump.write(msg);
                long totalVariantsWritten = 0l;
                String chrFileName = fileOutput.replace("#CHR#", chr);
                chrFiles.add(chrFileName);
                File chrFile = new File(chrFileName);
                if( chrFile.exists() ) {
                    msg = chrFileName+" already exists";
                    logger.info(msg);
                    dump.write(msg+"\n");
                    return chrFiles;
                }
                //BufferedWriter out = createGZip(chrFileName);
                BufferedWriter out = new BufferedWriter(new FileWriter(new File(chrFileName)));

                JsonFactory jf = new JsonFactory();
                JsonGenerator jsonGenerator = jf.createGenerator(out);
                jsonGenerator.useDefaultPrettyPrinter();
                jsonGenerator.writeStartObject();
                jsonGenerator.writeFieldName("variants");
                jsonGenerator.writeStartArray();
                jsonGenerator.writeRaw("\n");


                String urlTemplate = getUrl().replace("#CHR#", chr).replace("#SPECIES#", getGroupLabels().get(mapKey));

                int variantsWritten = 0;
                for( int i=1; i<chrLen; i+=CHUNK_SIZE ) {
                    String url = urlTemplate.replace("#START#", Integer.toString(i)).replace("#STOP#", Integer.toString(i+CHUNK_SIZE-1));
                    logger.info(url);
                    fd.setExternalFile(url);
                    fd.setLocalFile("tmp/z/" +chr+"/"+ (++fileNr) + ".json.gz");
                    fd.setUseCompression(true);

                    String localFile = fd.downloadNew();


                    BufferedReader jsonRaw = Utils.openReader(localFile);
                    JsonObject obj = (JsonObject) Jsoner.deserialize(jsonRaw);
                    JsonArray arr = (JsonArray) obj.get("response");
                    for( int j=0; j<arr.size(); j++) {
                        JsonObject response = (JsonObject) arr.get(j);
                        int numResults = ((BigDecimal) response.get("numResults")).intValueExact();
                        int numTotalResults = ((BigDecimal) response.get("numTotalResults")).intValueExact();
                        if( numResults<numTotalResults ) {
                            logger.info("*** serious problem: numResults<numTotalResults");
                            dump.write("*** serious problem: numResults<numTotalResults\n");
                        }

                        Double progressInPercent = (100.0*i)/(chrLen);
                        String progress = ",  "+String.format("%.1f%%", progressInPercent);
                        msg = "  chr"+chr+":"+i+"-"+(i+CHUNK_SIZE-1)+"   variants:"+numResults+" chr total:"+variantsWritten+progress+"\n";
                        logger.info(msg);
                        dump.write(msg);

                        if( numResults==0 ) {
                            continue;
                        }
                        JsonArray result = (JsonArray) response.get("result");
                        for( int k=0; k<result.size(); k++ ) {
                            JsonObject o = (JsonObject) result.get(k);
                            if( variantsWritten>0 ) {
                                jsonGenerator.writeRaw(",\n");
                            }
                            jsonGenerator.writeRaw(o.toJson());
                            variantsWritten++;

                            // finish test
                            if( variantsWritten>=Integer.MAX_VALUE ) {
                                jsonGenerator.writeEndArray();
                                jsonGenerator.writeEndObject();
                                jsonGenerator.close();
                                out.close();
                                dump.close();
                                throw new Exception("BREAK");
                            }
                        }
                    }
                }
                totalVariantsWritten += variantsWritten;
                msg = "============\n";
                msg += "chr"+chr+", variants written: "+Utils.formatThousands(variantsWritten)
                        +",  total variants written: "+Utils.formatThousands(totalVariantsWritten)+"\n";
                msg += "============\n\n";
                logger.info(msg);
                dump.write(msg);
                dump.flush();

                jsonGenerator.writeEndArray();
                jsonGenerator.writeEndObject();
                jsonGenerator.close();

                out.close();

                // delete all temporary files
                File dirZ = new File("tmp/z/"+chr);

                for( File file: dirZ.listFiles() ) {
                    if (!file.isDirectory() && file.getName().endsWith(".json.gz"))
                        file.delete();
                }
            }catch(Exception e){e.printStackTrace();} //} );
        return chrFiles;
    }

    void processFile( String fname, int mapKey, BufferedWriter dump, List<EvaAPI> evaList ) throws Exception {

        int snpsWithoutAccession = 0;

        BufferedReader br = Utils.openReader(fname);
        JsonFactory jf = new JsonFactory();
        JsonParser jp = jf.createParser(br);

        // keep track on the current depth of 'json objects':
        // new DB_SNP objects starts at 'jsonObjectDepth == 2'
        int jsonObjectDepth = 0;

        Map<String,Integer> unknownFields = new TreeMap<>();
        Map<Integer,Integer> snpLengths = new HashMap<>();
        EvaAPI eva = null;
        JsonToken token;
        while( (token=jp.nextToken()) != null) {
            if( token == JsonToken.START_OBJECT ) {
                jsonObjectDepth++;

                if( jsonObjectDepth==2 ) {
                    //System.out.println("start new db_snp object");
                    eva = new EvaAPI();
                    eva.setMapKey(mapKey);
                }
            } else if( token == JsonToken.END_OBJECT ) {
                jsonObjectDepth--;

                if( jsonObjectDepth==1 ) {
                    if( !save(eva, dump, evaList) ) {
                        snpsWithoutAccession++;
                    }
                }
            }

            String fieldName = jp.getCurrentName();
            if( fieldName==null ) {
                continue;
            }
            switch( fieldName ) {
                case "alternate":
                case "altAllele":
                    String allele = jp.nextTextValue();
                    if( eva.getAllele()!=null ) {
                        if( !allele.equals(eva.getAllele()) ) {
                            throw new Exception("allele override");
                        }
                    } else {
                        eva.setAllele(allele);
                    }
                    break;
                case "mafAllele":
                    String mafAllele = jp.nextTextValue();
                    if( eva.getMafAllele()!=null ) {
                        if( !mafAllele.equals(eva.getMafAllele()) ) {
                            // add new maf allele to 'mafAllele' property
                            if( !eva.getMafAllele().contains(mafAllele) ) {
                                eva.setMafAllele(eva.getMafAllele()+"/"+mafAllele);
                            }
                        }
                    } else {
                        eva.setMafAllele(mafAllele);
                    }
                    break;
                case "maf":
                    jp.nextToken();
                    eva.setMafFrequency(jp.getDoubleValue());
                    break;
                case "numSamples":
                    eva.setMafSampleSize(jp.nextIntValue(0));
                    break;
                case "refAllele":
                case "reference":
                    String refAllele = jp.nextTextValue();
                    if( eva.getRefAllele()!=null ) {
                        if( !refAllele.equals(eva.getRefAllele()) ) {
                            throw new Exception("ref allele override");
                        }
                    } else {
                        eva.setRefAllele(refAllele);
                    }
                    break;
                case "type":
                case "variantType":
                    String snpType = jp.nextTextValue();
                    if( eva.getSnpClass()!=null ) {
                        if( !snpType.equals(eva.getSnpClass()) ) {
                            throw new Exception("snp class override");
                        }
                    } else {
                        eva.setSnpClass(snpType);
                    }
                    break;
                case "format":
                    String format = jp.nextTextValue();
                    if( !format.equals("GT") ) {
                        throw new Exception("unexpected format: "+format);
                    }
                    break;
                case "GT":
                    String genotype = jp.nextTextValue();
                    // skip genotype "-1/-1"
                    if( genotype.equals("-1/-1") ) {
                        break;
                    }
                    if( eva.getGenotype()!=null ) {
                        if( !genotype.equals(eva.getGenotype()) ) {
                            // merge genotypes
                            if( !eva.getGenotype().contains(genotype) ) {
                                eva.setGenotype(eva.getGenotype()+", "+genotype);
                            }
                        }
                    } else {
                        eva.setGenotype(genotype);
                    }
                    break;
                case "eva-build":
                    eva.setSource("eva"+jp.nextTextValue());
                    break;
                case "chromosome":
                    eva.setChromosome(jp.nextTextValue());
                    break;
                case "start":
                    eva.setPosition(jp.nextIntValue(0));
                    break;
                case "length":
                    int snpLen = jp.nextIntValue(0);
                    Integer count = snpLengths.get(snpLen);
                    if( count==null ) {
                        count = 1;
                    } else {
                        count++;
                    }
                    snpLengths.put(snpLen, count);
                    break;
                case "ids":
                    if( jp.nextToken() != JsonToken.START_ARRAY ) {
                        throw new Exception("unexpected: no array for ids");
                    }
                    while( jp.nextToken()!= JsonToken.END_ARRAY ) {
                        String id = jp.getText();
                        if( id.startsWith("rs") ) {
                            eva.setEvaName(id);
                        }
                    }
                    break;
                case "genomic":
                    if( jp.nextToken() != JsonToken.START_ARRAY ) {
                        throw new Exception("unexpected: no array for genomic");
                    }
                    while( jp.nextToken()!= JsonToken.END_ARRAY ) {
                        String id = jp.getText();
                        if( id.contains(":g.") ) {
                            if( eva.getHgvs()==null ) {
                                eva.setHgvs(new ArrayList<>());
                            }
                            eva.getHgvs().add(id);
                        }
                    }
                    break;

                case "altAlleleCount": // ignore this for a moment
                case "altAlleleFreq": // ignore this for a moment
                case "mgf": // ignore this for a moment
                case "missingAlleles": // ignore this for a moment
                case "missingGenotypes": // ignore this for a moment
                case "refAlleleCount": // ignore this for a moment
                case "refAlleleFreq": // ignore this for a moment
                case "transition": // ignore this for a moment
                case "transversion": // ignore this for a moment

                case "attributes":
                case "cohortStats":
                case "end":
                case "fileId":
                case "hgvs":
                case "samplesData":
                case "sourceEntries":
                case "studyId":
                case "variants":
                    break;

                default:
                    Integer count2 = unknownFields.get(fieldName);
                    if( count2 == null ) {
                        count2 = 1;
                    } else {
                        count2++;
                    }
                    unknownFields.put(fieldName, count2);
                    //System.out.println("unknown field "+fieldName);
            }

        }

        jp.close();
        br.close();

        // insert remaining snps
        save(null, dump, evaList);
        logger.info("DAO: total rows inserted: "+rowsInserted);
        if( snpsWithoutAccession>0 ) {
            logger.info("### WARN: skipped snps without accession: " + snpsWithoutAccession);
        }

        // dump unknown fields
        logger.info("\n");
        logger.info("unknown fields: ");
        for( Map.Entry<String,Integer> entry: unknownFields.entrySet() ) {
            logger.info("   "+entry.getKey()+" : "+entry.getValue());
        }

        // dump snp lengthd
        logger.info("\n");
        logger.info("snp lengths distribution: ");
        for( Map.Entry<Integer,Integer> entry: snpLengths.entrySet() ) {
            logger.info("   "+entry.getKey()+" : "+entry.getValue());
        }
        return;//System.exit(-1);
    }

    boolean save(EvaAPI eva, BufferedWriter dumper, List<EvaAPI> evaList) throws Exception {

        if( eva!=null ) {
            if( eva.getEvaName()==null ) {
                logger.info("## no RS id for "+eva.getSnpClass()+" at CHR"+eva.getChromosome()+":"+eva.getPosition());
                return false;
            }

            evaList.add(eva);
            String builder = eva.getEvaName() + '\t' +
                    eva.getChromosome() + '\t' +
                    eva.getPosition() + '\t' +
                    eva.getSnpClass() + '\t' +
                    eva.getGenotype() + '\t' +
                    eva.getSource() + '\t' +
                    eva.getAllele() + '\t' +
                    eva.getMafFrequency() + '\t' +
                    eva.getMafSampleSize() + '\t' +
                    eva.getMafAllele() + '\t' +
                    eva.getRefAllele() + '\t' +
                    '\n';
            dumper.write(builder);

        }
        /*if( eva==null || evaList.size()>=100000 ) {
            rowsInserted += evaList.size();
            dao.insert(evaList);
            evaList.clear();
            logger.info("DAO: rows inserted "+rowsInserted);
        }*/
        return true;
    }

    public void insertAndDeleteEvaObjectsByKeyAndChromosome(ArrayList<Eva> incoming, int mapKey, String chromosome) throws Exception {
        List<Eva> inRGD = dao.getEvaObjectsFromMapKeyAndChromosome(mapKey,chromosome);
        logger.debug("  Inserting and deleting Eva Objects");
        // determines new objects to be inserted
        Collection<Eva> tobeInserted = CollectionUtils.subtract(incoming, inRGD);
        if (!tobeInserted.isEmpty()) {
            logger.info("   New Eva objects to be inserted in chromosome "+chromosome+": " + tobeInserted.size());
            dao.insertEva(tobeInserted);
            tobeInserted.clear();
        }

        // determines old objects to be deleted
        Collection<Eva> tobeDeleted = CollectionUtils.subtract(inRGD, incoming);
        if (!tobeDeleted.isEmpty()) {
            logger.info("   Old Eva objects to be deleted in chromosome "+chromosome+": " + tobeDeleted.size());
            for(Eva eva : tobeDeleted){
                dao.deleteEva(eva.getEvaId());
            }
            tobeDeleted.clear();
        }

        Collection<Eva> matching = CollectionUtils.intersection(inRGD, incoming);
        int matchingEVA = matching.size();
        if (matchingEVA != 0) {
            logger.info("   Eva objects that are matching in chromosome "+chromosome+": " + matchingEVA);
            matching.clear();
        }
    }

    public void setGroupLabels(Map<Integer, String> groupLabels) {
        this.groupLabels = groupLabels;
    }

    public Map<Integer, String> getGroupLabels() {
        return groupLabels;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
