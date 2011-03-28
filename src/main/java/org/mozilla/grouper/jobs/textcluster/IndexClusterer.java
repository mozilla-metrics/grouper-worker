package org.mozilla.grouper.jobs.textcluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.mozilla.grouper.jobs.Histogram;
import org.mozilla.grouper.model.BaseCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Domain logic: search-based in-memory clustering of tf-idf vectors.
 * See https://github.com/davedash/textcluster for a non-parallel implementation in python.
 * This implementation is very similar, differences:
 *  - processing in chunks
 *  - uses more sparse data structures (no zeroes from defaultdict)
 *  - uses a centrality model supposed to generate better clusters
 *    (textcluster assigns documents greedy which could produce sub-optimum results)
 *  - can use chunking with MergeClusterer
 * The original paper on canopy clustering already suggests using an inverted index for high
 * dimensions. This roughly builds on that idea.
 *
 * Ideas for full parallelization without using chunks:
 *   1. Build the inverted index in the map stage (term) -> (doc, weight)
 *   2. From reducer, emit partial cosine similarity: (doc, doc, term) -> (weight)
 *       --> Q: what to do when postings from the mappers become too large?
 *   3. Remap to the key (doc, doc) and value (score).
 *   4. Reduce: Sum the now sorted partial scores, use the top score and emit entries with lower
 *      id to the left (so each entry is emitted only once). These are our cluster members.
 *       --> Q: Can we use the followers/splicing with that (not activated all that often anyways)?
 */
public class IndexClusterer {

    private static final Logger log = LoggerFactory.getLogger(IndexClusterer.class);

    // Defaults
    public static final int BLOCK_SIZE = 40000;
    public static final double SIMILARITY_THRESHOLD = .08;
    public static final double REASSIGN_THRESHOLD = .3;
    public static final int MIN_DOCUMENT_LENGTH = 2;

    /** The tf-idf vectors from the input documents. */
    private List<Vector> vectors_;

    /** Stuff that was could not be clustered is collected here, and not touched on reset. */
    private final List<Vector> rest_ = new java.util.ArrayList<Vector>();

    /**
     * Inverted index that maps: term-index --> (doc-index --> weight)
     *
     * We do not use a sparse structure because we assume the load factor to be
     * roughly 0.5 to 1 (depending on the collection size).
     */
    private final List<Map<Integer, Double>> index_;

    private final double minTreshold_ = SIMILARITY_THRESHOLD;
    private final double reassignTreshold_ = REASSIGN_THRESHOLD;
    private final int blockSize_ = BLOCK_SIZE;
    private int n_;
    private int dictSize_;

    /** the size of the term dictionary = the maximum vector length = the index size */
    public IndexClusterer(int dictSize) {
        log.info(String.format("Creating index clusterer. Dictionary size: %d", dictSize));
        dictSize_ = dictSize;
        index_ = new java.util.ArrayList<Map<Integer, Double>>(dictSize);
        reset();
    }

    private void reset() {
        n_ = 0;
        for (int i = 0; i < index_.size(); ++i)
            index_.set(i, new java.util.HashMap<Integer, Double>());
        for (int i = index_.size(); i < dictSize_; ++i)
            index_.add(new java.util.HashMap<Integer, Double>());
        vectors_ = new java.util.LinkedList<Vector>();
    }

    /**
     * Returns <tt>null</tt> until BLOCK_SIZE vectors have been added.
     * Then, compute and return a clustering and reset the internal state.
     * */
    public List<BaseCluster> add(Vector next) {
        if (next.getNumNondefaultElements() < MIN_DOCUMENT_LENGTH) return null;
        ++n_;
        vectors_.add(next);
        if (next.size() > dictSize_) {
            for (int i = dictSize_; i < next.size(); ++i)
                index_.add(new java.util.HashMap<Integer, Double>());
            dictSize_ = next.size();
        }

        if (n_ < blockSize_) return null;

        createIndex();
        List<BaseCluster> clusters = createClusters();
        reset();
        return clusters;
    }

    /**
     * Force calculation of the cluster, e.g. where the collection is smaller than the
     * chunk size.
     */
    public List<BaseCluster> clusters() {
        createIndex();
        return createClusters();
    }

    public List<Vector> rest() {
        return rest_;
    }

    private void createIndex() {

        final long ts = System.currentTimeMillis();
        log.info("Creating inverted index...");
        int doc = 0;
        for (final Vector v : vectors_) {
            final Iterator<Element> it = v.iterateNonZero();
            while (it.hasNext()) {
                final Element e = it.next();
                final int term = e.index();
                final double weight = e.get();
                index_.get(term).put(doc, weight);
            }
            ++doc;
        }

        log.info(String.format("Index created. Took %d ms.",
                               dictSize_, System.currentTimeMillis() - ts));
    }

    private List<BaseCluster> createClusters() {
        final long ts1 = System.currentTimeMillis();
        log.info(String.format("1/3 Creating similarity relation for %d document vectors...", n_));

        // Save three things about each document:
        // The most similar other doc, the similarity to that doc, the overall "centricity"
        // of this doc. While it has no impact on the distribution of documents among clusters,
        // documents with the highest centricity will become cluster medoids.
        final int[] bestMatch = new int[n_];
        for (int i = 0; i < n_; ++i) bestMatch[i] = -1;
        final double[] bestScore = new double[n_];
        final double[] centricity = new double[n_];
        final List<Integer> used = new ArrayList<Integer>(n_);

        int reassignments = 0;

        {   Iterator<Vector> vs = vectors_.listIterator();
            int docIdx = 0;
            final Map<Integer, Double> similars = new HashMap<Integer, Double>();
            while (vs.hasNext()) {
                // Calculate score for each term.
                final Iterator<Element> it = vs.next().iterateNonZero();
                int a = 0;
                while (it.hasNext()) {
                    final Element e = it.next();
                    final int term = e.index();
                    final Map<Integer, Double> posting = index_.get(term);
                    final double weight = e.get();
                    for (Map.Entry<Integer, Double> match : posting.entrySet()) {
                        final Integer matchIdx = match.getKey();
                        if (matchIdx <= docIdx) continue; // these are already complete

                        // add cosine similarity for this dimension
                        final double termScore = weight * match.getValue().doubleValue();
                        ++a;
                        final Double pairScore = similars.get(matchIdx);
                        if (pairScore == null) similars.put(matchIdx, termScore);
                        else similars.put(matchIdx, pairScore.doubleValue() + termScore);
                    }
                }

                for (Map.Entry<Integer, Double> entry : similars.entrySet()) {
                    double score = entry.getValue();
                    if (score < minTreshold_) continue; // too far off
                    int matchIdx = entry.getKey();
                    if (matchIdx < docIdx) continue; // already computed
                    centricity[docIdx] += score;
                    centricity[matchIdx] += score;

                    if (bestMatch[matchIdx] != -1) {
                        if (score < reassignTreshold_ || bestScore[matchIdx] > score)
                            continue;
                        reassignments++;
                    }

                    bestMatch[matchIdx] = docIdx;
                    bestScore[matchIdx] = score;
                }

                if (centricity[docIdx] > 0) used.add(docIdx);
                else rest_.add(vectors_.get(docIdx));
                ++docIdx;
                if (docIdx % 5000 == 0) log.info("...{}", docIdx);
                similars.clear();
            }
        }

        // We introduce a total ordering by centricity ASC, index DESC.
        // The latter is desc for best stability when clusters are rebuilt.
        Integer[] toUse = used.toArray(new Integer[used.size()]);
        Arrays.sort(toUse, new Comparator<Integer>() {
            @Override
            public int compare(Integer docA, Integer docB) {
                double primary = centricity[docA] - centricity[docB];
                if (primary == 0) return docB - docA;
                return (primary < 0) ? -1 : 1;
            }
        });

        // Clusters will be assigned in order of ascending centricity...
        // Documents will be associated to their best matches bottom-up.
        // Clusters will then be assigned top down.
        log.info(String.format("    OK. %d reassignments. Using %d/%d elements. Took %sms.",
                               reassignments, toUse.length, n_, System.currentTimeMillis() - ts1));


        log.info("2/3 Creating followers...");
        final long ts2 = System.currentTimeMillis();
        // assign each doc to its best match by adding a back-link for cluster creation
        // :TODO: this can degenerate to n**2 worst case (everything is in the same cluster),
        //        because the backlinks are copied repeatedly. We need a datastructure that allows
        //        for splicing...
        @SuppressWarnings("unchecked")
        final ArrayList<Integer>[] followers = new ArrayList[n_];
        for (Integer docIdx : toUse) {
            final int idx = docIdx.intValue();
            final int target = bestMatch[idx];
            if (target == -1) continue;
            // Make sure everything is pointing "up" to the cluster medoid.
            if (centricity[target] > centricity[idx]) continue;
            if (centricity[target] == centricity[idx] && target < docIdx) continue;
            bestMatch[target] = idx;
            bestMatch[idx] = -1;
        }

        int splices = 0;
        for (Integer docIdx : toUse) {
            final int idx = docIdx.intValue();
            final int target = bestMatch[idx];
            if (target == -1) continue;

            if (followers[target] == null) followers[target] = new ArrayList<Integer>();
            followers[target].add(docIdx);

            // move all followers to the element we follow ourselves
            // here we want to splice instead:
            if (followers[idx] != null) {
                splices++;
                followers[target].addAll(followers[idx]);
            }
            followers[idx] = null;
        }
        log.info(String.format("    Backlinks created. Took %sms. Spliced %d times.",
                               splices, System.currentTimeMillis() - ts2));


        log.info("3/3. Creating clusters, sorted by size desc:");
        long ts3 = System.currentTimeMillis();

        Histogram h = new Histogram();

        List<BaseCluster> clusters = new java.util.ArrayList<BaseCluster>();
        for (Integer docIdx : toUse) {
            final int idx = docIdx.intValue();
            if (followers[idx] == null) continue;
            List<Vector> followersList = new ArrayList<Vector>(followers[idx].size());
            List<Double> similarityList = new ArrayList<Double>(followers[idx].size());
            for (int followerIdx : followers[idx]) {
                followersList.add(vectors_.get(followerIdx));
                similarityList.add(Double.valueOf(bestScore[followerIdx]));
            }
            for (int i=0; i < followersList.size(); ++i) h.add(followersList.size());

            clusters.add(new BaseCluster(vectors_.get(idx), followersList, similarityList));
        }

        log.info(String.format("Size distribution: %s", h));
        log.info(String.format("    Done. Created and sorted %d clusters in %dms.",
                               clusters.size(), System.currentTimeMillis() - ts3));

        return clusters;
    }

}
