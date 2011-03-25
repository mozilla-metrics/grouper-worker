package org.mozilla.grouper.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.mozilla.grouper.base.Config;
import org.mozilla.grouper.model.CollectionRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizeDocuments extends AbstractCollectionTool {

    private static final Logger log = LoggerFactory.getLogger(VectorizeDocuments.class);

    public static String NAME = "vectorize";

    public VectorizeDocuments(Config conf, Configuration hadoopConf) {
        super(conf, hadoopConf);
    }

    @Override
    protected String name() { return NAME; }

    @Override
    protected int run(CollectionRef collection, long timestamp) throws Exception {
        final Configuration hadoopConf = getConf();
        AbstractCollectionTool source = new ExportDocuments(conf_, hadoopConf);
        final Path inputDir = new Path(source.outputDir(collection, timestamp));
        final Path outputDir = new Path(outputDir(collection, timestamp));


        // The API usage here is taken from the Mahout utility SparseVectorsFromSequenceFiles,
        // which serves a very similar purpose (but as a command line utility).
        // :TODO: Set them from the per-collection configuration.

        Class<? extends Analyzer> analyzerClass = DefaultAnalyzer.class;
        Path tokenizedPath = new Path(outputDir,
                                      DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
        DocumentProcessor.tokenizeDocuments(inputDir, analyzerClass, tokenizedPath, hadoopConf);

        int chunkSize = 100;
        int minSupport = 2;
        int maxNGramSize = 1;
        float minLLRValue = LLRReducer.DEFAULT_MIN_LLR;
        log.info("Minimum LLR value: {}", minLLRValue);

        int reduceTasks = 1;
        log.info("Number of reduce tasks: {}", reduceTasks);

        // TODO: not sure yet if we need this
        boolean namedVectors = false;
        boolean sequentialAccessOutput = true;

        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDir, hadoopConf,
                                                        minSupport, maxNGramSize, minLLRValue,
                                                        -1.0f, false, reduceTasks, chunkSize,
                                                        sequentialAccessOutput, namedVectors);

        // totally arbitrary:
        float norm = 2.0f;

        boolean logNormalize = false;
        // minimum number of documents a term appears in for it to be considered
        int minDf = 1;
        // max percentage of docs before term is considered a stopword
        int maxDFPercent = 90;

        // TODO: Can we do this and still have efficiently updateable clusters?
        //       ...do we need to update all vectors after adding a document?
        TFIDFConverter.processTfIdf(new Path(outputDir,
                                             DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                                    outputDir, hadoopConf, chunkSize, minDf, maxDFPercent, norm,
                                    logNormalize, sequentialAccessOutput, namedVectors,
                                    reduceTasks);
        return 0;
    }

}
