/** One entry from the store's catalog, as returned by GET /items. */
public record CatalogItem(String sku, String name, double price) {
}
