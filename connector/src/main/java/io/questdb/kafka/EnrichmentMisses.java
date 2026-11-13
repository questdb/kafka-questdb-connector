package io.questdb.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The enrichment keys that found no mapping, kept so that they are logged when they change
 * rather than on every refresh: a key set that is partial for good is reported once. A key is
 * tracked from the first record it misses on until a refresh maps it. Owned by one task thread.
 */
final class EnrichmentMisses {
    // That many distinct misses suggests a key field unique per record. The cap bounds the
    // memory to about 1 MB for short keys.
    static final int MAX_TRACKED_KEYS = 10_000;
    private static final int MAX_LOGGED_KEYS = 20;
    private static final Logger log = LoggerFactory.getLogger(EnrichmentMisses.class);

    private final String keyField;
    private final Set<String> unmapped = new HashSet<>();
    // the first new keys since the previous report, for the DEBUG line
    private final List<String> newKeys = new ArrayList<>();
    private int added;
    private boolean overflowed;
    private boolean overflowReported;
    private int resolved;
    private long unusable;
    private boolean unusableReported;
    private long absent;
    private boolean absentReported;

    EnrichmentMisses(String keyField) {
        this.keyField = keyField;
    }

    /**
     * Called for every record whose key has no mapping. Costs one hash probe: the key is the
     * instance the lookup has just hashed, and String caches its hash.
     */
    void miss(String key) {
        if (unmapped.size() < MAX_TRACKED_KEYS) {
            if (unmapped.add(key)) {
                added++;
                if (newKeys.size() < MAX_LOGGED_KEYS) {
                    newKeys.add(key);
                }
            }
        } else if (!overflowed && !unmapped.contains(key)) {
            overflowed = true;
        }
    }

    /**
     * Called for every record whose key field holds a value no lookup key can match: a struct,
     * map, array, bytes or a decimal. Such a record is neither a hit nor a miss.
     */
    void unusable() {
        unusable++;
    }

    /**
     * Called for every record with no value in the key field: the field is missing or null.
     * When every record is, the key field is most likely misnamed.
     */
    void absent() {
        absent++;
    }

    /** Forgets the keys a freshly loaded mapping covers. */
    void resolve(Map<String, ?> mapping) {
        for (Iterator<String> it = unmapped.iterator(); it.hasNext(); ) {
            if (mapping.containsKey(it.next())) {
                it.remove();
                resolved++;
            }
        }
    }

    /**
     * Logs what changed since the previous call, if anything. Counts go to INFO; key values only
     * to DEBUG, since Connect does not log record contents by default either.
     */
    void report() {
        if (overflowed && !overflowReported) {
            overflowReported = true;
            log.warn("More than {} distinct enrichment keys have no mapping; further ones are not tracked."
                            + " Check {}: a field unique per record, such as an event id, causes this",
                    MAX_TRACKED_KEYS, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG);
        }
        if (added > 0 || resolved > 0) {
            log.info("Enrichment lookup: {} key(s) have no mapping ({} new, {} mapped since the previous report)",
                    unmapped.size(), added, resolved);
            if (!newKeys.isEmpty()) {
                log.debug("Enrichment keys newly without a mapping (at most {} shown): {}", MAX_LOGGED_KEYS, newKeys);
            }
            added = 0;
            resolved = 0;
            newKeys.clear();
        }
        if (unusable > 0) {
            // The cause is usually the configuration, which does not change while the task runs.
            if (!unusableReported) {
                unusableReported = true;
                log.warn("{} record(s) had a value in {} '{}' that cannot be a lookup key; only text, integers,"
                                + " floating-point numbers and booleans can. Those records are not enriched;"
                                + " later ones are logged at DEBUG",
                        unusable, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, keyField);
            } else {
                log.debug("{} more record(s) had a value in {} '{}' that cannot be a lookup key",
                        unusable, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, keyField);
            }
            unusable = 0;
        }
        if (absent > 0) {
            if (!absentReported) {
                absentReported = true;
                log.warn("{} record(s) had no value in {} '{}'; those records are not enriched. If every record"
                                + " has that field, check the name: nested names are joined with '_' and start with"
                                + " the value or key prefix. Later ones are logged at DEBUG",
                        absent, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, keyField);
            } else {
                log.debug("{} more record(s) had no value in {} '{}'",
                        absent, QuestDBSinkConnectorConfig.ENRICHMENT_KEY_FIELD_CONFIG, keyField);
            }
            absent = 0;
        }
    }
}
