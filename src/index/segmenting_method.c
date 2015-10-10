#include "../destor.h"
/*
 * c == NULL indicates the end and return the segment.
 * If a segment boundary is found, return the segment;
 * else return NULL.
 */
struct segment* (*segmenting)(struct chunk *c);

/*
 * Used by SiLo and Block Locality Caching.
 */
static struct segment* segment_fixed(struct chunk * c) {
    static struct segment* tmp;
    if (tmp == NULL)
        tmp = new_segment();

    if (c == NULL)
        /* The end of stream */
        return tmp;

    g_sequence_append(tmp->chunks, c);
    if (CHECK_CHUNK(c, CHUNK_FILE_START) 
            || CHECK_CHUNK(c, CHUNK_FILE_END))
        /* FILE_END */
        return NULL;

    /* a normal chunk */
    tmp->chunk_num++;

    if (tmp->chunk_num == destor.index_segment_algorithm[1]) {
        /* segment boundary */
        struct segment* ret = tmp;
        tmp = NULL;
        return ret;
    }

    return NULL;
}

/*
 * Used by Extreme Binning.
 */
static struct segment* segment_file_defined(struct chunk *c) {
    static struct segment* tmp;
    /*
     * For file-defined segmenting,
     * the end is not a new segment.
     */
    if (tmp == NULL)
        tmp = new_segment();

    if (c == NULL)
        return tmp;

    g_sequence_append(tmp->chunks, c);
    if (CHECK_CHUNK(c, CHUNK_FILE_END)) {
        struct segment* ret = tmp;
        tmp = NULL;
        return ret;
    } else if (CHECK_CHUNK(c, CHUNK_FILE_START)) {
        return NULL;
    } else {
        /* a normal chunk */
        tmp->chunk_num++;
        return NULL;
    }
}

/*
 * Used by Sparse Index.
 */
static struct segment* segment_content_defined(struct chunk *c) {
    static struct segment* tmp;

    if (tmp == NULL)
        tmp = new_segment();

    if (c == NULL)
        /* The end of stream */
        return tmp;

    if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)) {
        g_sequence_append(tmp->chunks, c);
        return NULL;
    }

    /* Avoid too small segment. */
    if (tmp->chunk_num < destor.index_segment_min) {
    	g_sequence_append(tmp->chunks, c);
        tmp->chunk_num++;
        return NULL;
    }

    int *head = (int*)&c->fp[16];
    if ((*head) % destor.index_segment_algorithm[1] == 0) {
        struct segment* ret = tmp;
        tmp = new_segment();
        g_sequence_append(tmp->chunks, c);
        tmp->chunk_num++;
        return ret;
    }

    g_sequence_append(tmp->chunks, c);
    tmp->chunk_num++;
    if (tmp->chunk_num >= destor.index_segment_max){
        struct segment* ret = tmp;
        tmp = new_segment();
        return ret;
    }

    return NULL;
}

void init_segmenting_method(){
    switch (destor.index_segment_algorithm[0]) {
        case INDEX_SEGMENT_FIXED:
            segmenting = segment_fixed;
            break;
        case INDEX_SEGMENT_CONTENT_DEFINED:
            segmenting = segment_content_defined;
            break;
        case INDEX_SEGMENT_FILE_DEFINED:
            segmenting = segment_file_defined;
            break;
        default:
            fprintf(stderr, "Invalid segment algorithm!\n");
            exit(1);
    }

}
