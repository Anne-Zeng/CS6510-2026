import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks random catalog items to "scan" during the simulation.
 *
 * A pure uniform-random pick across a 2,000-item catalog would make the
 * store's "popular items" feature meaningless to test - every item would be
 * scanned about equally often. Real retail sales follow something close to
 * a Zipf distribution (a small number of items account for a large share of
 * sales), so this sampler weights the catalog the same way: item at rank i
 * (1-based) gets weight 1/i. With a 2,000-item catalog, the top ~20 items
 * end up receiving a clearly visible, testable share of scans.
 */
public final class ItemSampler {

    private final List<CatalogItem> catalogInRankOrder;
    private final double[] cumulativeWeights;
    private final double totalWeight;

    public ItemSampler(List<CatalogItem> catalog) {
        // Catalog order, as returned by the server, defines "rank" for the
        // Zipf skew. This is arbitrary but stable for a given catalog, which
        // is all that's needed to make popularity results reproducible.
        this.catalogInRankOrder = catalog;
        this.cumulativeWeights = new double[catalog.size()];
        double running = 0.0;
        for (int i = 0; i < catalog.size(); i++) {
            running += 1.0 / (i + 1); // rank is 1-based
            cumulativeWeights[i] = running;
        }
        this.totalWeight = running;
    }

    public CatalogItem next() {
        double target = ThreadLocalRandom.current().nextDouble() * totalWeight;
        int lo = 0, hi = cumulativeWeights.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cumulativeWeights[mid] < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return catalogInRankOrder.get(lo);
    }
}
