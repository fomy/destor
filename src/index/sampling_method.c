#include "../destor.h"
#include "index.h"

/*
 * Sampling features for a chunk sequence.
 */
GHashTable* (*sampling)(GSequence *chunks, int32_t chunk_num);

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable* index_sampling_min(GSequence *chunks, int32_t chunk_num) {

    chunk_num = (chunk_num == 0) ? g_sequence_get_length(chunks) : chunk_num;
    int feature_num = 1;
    if (destor.index_sampling_method[1] != 0
            && chunk_num > destor.index_sampling_method[1]) {
        /* Calculate the number of features we need */
        int remain = chunk_num % destor.index_sampling_method[1];
        feature_num = chunk_num / destor.index_sampling_method[1];
        feature_num = (remain * 2 > destor.index_sampling_method[1]) ?
            feature_num + 1 : feature_num;
    }

    GSequence *candidates = g_sequence_new(free);
    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        /* iterate the queue */
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) 
                || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        if (g_sequence_get_length(candidates) < feature_num
                || memcmp(&c->fp, g_sequence_get(
                        g_sequence_iter_prev(
                            g_sequence_get_end_iter(candidates))),
                    sizeof(fingerprint)) < 0) {
            /* insufficient candidates or new candidate */
            fingerprint *new_candidate = (fingerprint*) malloc(
                    sizeof(fingerprint));
            memcpy(new_candidate, &c->fp, sizeof(fingerprint));
            g_sequence_insert_sorted(candidates, new_candidate,
                    g_fingerprint_cmp, NULL);
            if (g_sequence_get_length(candidates) > feature_num) {
                g_sequence_remove(
                        g_sequence_iter_prev(
                            g_sequence_get_end_iter(candidates)));
            }
        }
    }

    GHashTable * features = g_hash_table_new_full(g_feature_hash,
            g_feature_equal, free, NULL);

    while (g_sequence_get_length(candidates) > 0) {
        fingerprint *candidate = g_sequence_get(
                g_sequence_get_begin_iter(candidates));
        char* feature = malloc(destor.index_key_size);
        memcpy(feature, candidate, destor.index_key_size);
        g_hash_table_insert(features, feature, NULL);
        g_sequence_remove(g_sequence_get_begin_iter(candidates));
    }
    g_sequence_free(candidates);

    if (g_hash_table_size(features) == 0) {
        WARNING("Dedup phase: An empty segment and thus no min-feature is selected!");
        char* feature = malloc(destor.index_key_size);
        memset(feature, 0xff, destor.index_key_size);
        g_hash_table_insert(features, feature, NULL);
    }

    return features;
}

/*
 * Used by Extreme Binning and Silo.
 */
static GHashTable* index_sampling_optimized_min(GSequence *chunks,
        int32_t chunk_num) {

    chunk_num = (chunk_num == 0) ? g_sequence_get_length(chunks) : chunk_num;
    int feature_num = 1;
    if (destor.index_sampling_method[1] != 0
            && chunk_num > destor.index_sampling_method[1]) {
        /* Calculate the number of features we need */
        int remain = chunk_num % destor.index_sampling_method[1];
        feature_num = chunk_num / destor.index_sampling_method[1];
        feature_num =
            (remain * 2 > destor.index_sampling_method[1]) ?
            feature_num + 1 : feature_num;
    }

    struct anchor {
        fingerprint anchor;
        fingerprint candidate;
    };

    int off = 8;
    fingerprint prefix[off + 1];
    int count = 0;
    memset(prefix, 0xff, sizeof(fingerprint) * (off + 1));

    /* Select anchors */
    GSequence *anchors = g_sequence_new(free);

    GSequenceIter* iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter* end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        /* iterate the queue */
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        memmove(&prefix[1], prefix, sizeof(fingerprint) * (off));
        memcpy(&prefix[0], &c->fp, sizeof(fingerprint));
        if (g_sequence_get_length(anchors) < feature_num
                || memcmp(&c->fp,
                    g_sequence_get(
                        g_sequence_iter_prev(
                            g_sequence_get_end_iter(anchors))),
                    sizeof(fingerprint)) < 0) {
            /* insufficient candidates or new candidate */
            struct anchor *new_anchor = (struct anchor*) malloc(
                    sizeof(struct anchor));
            memcpy(new_anchor->anchor, &c->fp, sizeof(fingerprint));
            if (count >= off) {
                memcpy(&new_anchor->candidate, &prefix[off],
                        sizeof(fingerprint));
            } else {
                memcpy(&new_anchor->candidate, &prefix[count],
                        sizeof(fingerprint));
            }

            g_sequence_insert_sorted(anchors, new_anchor, g_fingerprint_cmp,
                    NULL);
            if (g_sequence_get_length(anchors) > feature_num)
                g_sequence_remove(
                        g_sequence_iter_prev(g_sequence_get_end_iter(anchors)));

        }
        count++;
    }

    GHashTable * features = g_hash_table_new_full(g_feature_hash,
            g_feature_equal, free, NULL);

    while (g_sequence_get_length(anchors) > 0) {
        struct anchor *a = g_sequence_get(g_sequence_get_begin_iter(anchors));

        char* feature = malloc(destor.index_key_size);
        memcpy(feature, &a->candidate, destor.index_key_size);

        g_hash_table_insert(features, feature, NULL);
        g_sequence_remove(g_sequence_get_begin_iter(anchors));
    }
    g_sequence_free(anchors);

    if (g_hash_table_size(features) == 0) {
        WARNING("Dedup phase: An empty segment and thus no min-feature is selected!");
        char* feature = malloc(destor.index_key_size);
        memset(feature, 0xff, destor.index_key_size);
        g_hash_table_insert(features, feature, NULL);
    }

    return features;
}

/*
 * Used by Sparse Indexing.
 */
static GHashTable* index_sampling_random(GSequence *chunks, int32_t chunk_num) {
    assert(destor.index_sampling_method[1] != 0);
    GHashTable * features = g_hash_table_new_full(g_feature_hash,
            g_feature_equal, free, NULL);

    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        /* iterate the queue */
        struct chunk* c = g_sequence_get(iter);

        if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
            continue;

        int *head = (int*)&c->fp[16];
        if ((*head) % destor.index_sampling_method[1] == 0) {
            if (!g_hash_table_contains(features, &c->fp)) {
                char *new_feature = malloc(destor.index_key_size);
                memcpy(new_feature, &c->fp, destor.index_key_size);
                g_hash_table_insert(features, new_feature, NULL);
            }
        }
    }

    if (g_hash_table_size(features) == 0) {
        /* No feature? */
        WARNING("Dedup phase: no features are sampled");
        char *new_feature = malloc(destor.index_key_size);
        memset(new_feature, 0x00, destor.index_key_size);
        g_hash_table_insert(features, new_feature, NULL);
    }
    return features;

}

static GHashTable* index_sampling_uniform(GSequence *chunks, int32_t chunk_num) {
    assert(destor.index_sampling_method[1] != 0);
    GHashTable * features = g_hash_table_new_full(g_feature_hash,
            g_feature_equal, free, NULL);
    int count = 0;

    GSequenceIter *iter = g_sequence_get_begin_iter(chunks);
    GSequenceIter *end = g_sequence_get_end_iter(chunks);
    for (; iter != end; iter = g_sequence_iter_next(iter)) {
        struct chunk *c = g_sequence_get(iter);
        /* Examine whether fp is a feature */
        if (count % destor.index_sampling_method[1] == 0) {
            if (!g_hash_table_contains(features, &c->fp)) {
                char *new_feature = malloc(destor.index_key_size);
                memcpy(new_feature, &c->fp, destor.index_key_size);
                g_hash_table_insert(features, new_feature, NULL);
            }
        }
        count++;
    }

    if (g_hash_table_size(features) == 0) {
        /* No feature? Empty segment.*/
        assert(chunk_num == 0);
        WARNING("Dedup phase: An empty segment and thus no uniform-feature is selected!");
        char *new_feature = malloc(destor.index_key_size);
        memset(new_feature, 0x00, destor.index_key_size);
        g_hash_table_insert(features, new_feature, NULL);
    }
    return features;
}

void init_sampling_method(){
    switch (destor.index_sampling_method[0]) {
        case INDEX_SAMPLING_RANDOM:
            sampling = index_sampling_random;
            break;
        case INDEX_SAMPLING_OPTIMIZED_MIN:
            sampling = index_sampling_optimized_min;
            break;
        case INDEX_SAMPLING_MIN:
            sampling = index_sampling_min;
            break;
        case INDEX_SAMPLING_UNIFORM:
            sampling = index_sampling_uniform;
            break;
        default:
            fprintf(stderr, "Invalid sampling method!\n");
            exit(1);
    }
}
