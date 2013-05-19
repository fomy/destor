#include "sparse_index.h"
#include "../jcr.h"

extern char working_path[];
extern void send_feature(Chunk *chunk);

int32_t segment_bits = 11;
int32_t sample_bits = 8;
int32_t champions_number = 2;

static GHashTable *sparse_index; //Fingerprint (hook) -> manifest id sequence

static int64_t manifest_volume_length;
static int64_t manifest_number;
static int mvol_fd = -1;

static Manifest *new_manifest = NULL;

static BOOL and_bits(unsigned char hash[], int32_t bits) {
	int32_t remainder = bits % 8;
	int32_t quotient = bits / 8;
	int i = 0;
	for (; i < quotient; ++i) {
		if (hash[i] != 0)
			return FALSE;
	}
	if (remainder) {
		if ((hash[i] >> (8 - remainder)) != 0)
			return FALSE;
	}
	return TRUE;
}

static BOOL manifest_volume_init() {
	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/manifest_volume");

	if ((mvol_fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open manifest volume");
		return FALSE;
	}

	struct stat fileinfo;
	stat(filename, &fileinfo);
	size_t filesize = fileinfo.st_size;

	if (filesize > 0) {
		read(mvol_fd, &manifest_volume_length, 8);
		read(mvol_fd, &manifest_number, 8);
	} else {
		/* First 16 bytes are length and number. */
		manifest_volume_length = 16;
		manifest_number = 0;
	}

}

static void manifest_volume_destroy() {
	lseek(mvol_fd, 0, SEEK_SET);
	write(mvol_fd, &manifest_volume_length, 8);
	write(mvol_fd, &manifest_number, 8);

	if (mvol_fd > 0)
		close(mvol_fd);
	mvol_fd = -1;
}
/* ascending */
static gint manifest_cmp(Manifest *a, Manifest *b, gpointer user_data) {
	return (a->id >> 0x18) - (b->id >> 0x18);
}

/* descending */
static gint manifest_cmp_length(Manifest *a, Manifest* b, gpointer user_data) {
	if (g_sequence_get_length(b->matched_hooks)
			== g_sequence_get_length(a->matched_hooks)) {
		/* we prefer recent manifests */
		return (b->id >> 0x18) - (a->id >> 0x18);
	}
	return g_sequence_get_length(b->matched_hooks)
			- g_sequence_get_length(a->matched_hooks);
}

static void unscore(Manifest *base, Manifest *dest) {
	GSequenceIter *iter = g_sequence_get_begin_iter(base->matched_hooks);
	while (!g_sequence_iter_is_end(iter)) {
		if (g_sequence_get_length(dest->matched_hooks) == 0)
			break;
		GSequenceIter *remove_iter = g_sequence_lookup(dest->matched_hooks,
				g_sequence_get(iter), g_fingerprint_cmp, NULL );
		if (remove_iter) {
			g_sequence_remove(remove_iter);
		}
		iter = g_sequence_iter_next(iter);
	}
}

static void free_manifest(Manifest *manifest) {
	if (manifest->matched_hooks)
		g_sequence_free(manifest->matched_hooks);
	if (manifest->fingers)
		g_hash_table_destroy(manifest->fingers);
	free(manifest);
}

/*
 * Select champions.
 * hooks is the sampled features of a segment.
 */
static GSequence* select_champions(EigenValue *hooks) {
	int i = 0;
	/* manifest sequence */
	GSequence *champions = g_sequence_new(free_manifest);
	for (; i < hooks->value_num; ++i) {
		/* Get all IDs of manifests associated with hooks. */
		GSequence *id_seq = g_hash_table_lookup(sparse_index,
				&hooks->values[i]);
		if (id_seq == NULL )
			continue;
		GSequenceIter *id_seq_iter = g_sequence_get_begin_iter(id_seq);
		while (!g_sequence_iter_is_end(id_seq_iter)) {
			int64_t *id = g_sequence_get(id_seq_iter);
			GSequenceIter *manifest_iter = g_sequence_lookup(champions, id,
					manifest_cmp, NULL );
			Manifest *manifest = NULL;
			if (manifest_iter)
				manifest = g_sequence_get(manifest_iter);
			else if (manifest == NULL ) {
				/* Construct a new manifest */
				manifest = (Manifest*) malloc(sizeof(Manifest));
				manifest->id = *id;
				manifest->matched_hooks = g_sequence_new(free);
				manifest->fingers = NULL;

				g_sequence_insert_sorted(champions, manifest, manifest_cmp,
						NULL );

			}
			Fingerprint *matched_hook = (Fingerprint*) malloc(
					sizeof(Fingerprint));
			memcpy(matched_hook, &hooks->values[i], sizeof(Fingerprint));
			/* insert matched hook */
			g_sequence_insert_sorted(manifest->matched_hooks, matched_hook,
					g_fingerprint_cmp, NULL );

			id_seq_iter = g_sequence_iter_next(id_seq_iter);
		}
	}

	g_sequence_sort(champions, manifest_cmp_length, NULL );
	if (g_sequence_get_length(champions) > champions_number) {
		/* We now select the Top champion_number manifests. */
		GSequenceIter *base = g_sequence_get_begin_iter(champions);
		int i = 0;
		for (; i < champions_number; ++i) {
			GSequenceIter *next = g_sequence_iter_next(base);
			while (!g_sequence_iter_is_end(next)) {
				unscore(g_sequence_get(base), g_sequence_get(next));
				next = g_sequence_iter_next(next);
			}

			g_sequence_sort(champions, manifest_cmp_length, NULL );
			base = g_sequence_get_iter_at_pos(champions, i + 1);
			if (g_sequence_iter_is_end(base)) {
				dprint("It can't happen!");
			}
		}

		GSequenceIter *loser = g_sequence_get_iter_at_pos(champions,
				champions_number);
		g_sequence_remove_range(loser, g_sequence_get_end_iter(champions));
		if (g_sequence_get_length(champions) != champions_number)
			printf("%s, %d: %d != champions_number.", __FILE__, __LINE__,
					g_sequence_get_length(champions));
	}
	return champions;
}

static void load_manifest(Manifest *manifest) {
	int64_t offset = manifest->id >> 0x18;
	int32_t length = manifest->id & 0xffffff;
	/*printf("load id = %lld, %lld, %d\n", manifest->id, offset, length);*/
	char buffer[length];

	lseek(mvol_fd, offset, SEEK_SET);
	read(mvol_fd, buffer, length);

	char *p = buffer;
	if (manifest->id != *(int64_t*) p)
		dprint("inconsistent manifest ID!");
	p += 8;

	manifest->fingers = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, free);
	while ((p - buffer) < length) {
		Fingerprint *fingerprint = (Fingerprint*) malloc(sizeof(Fingerprint));
		ContainerId *cid = (ContainerId*) malloc(sizeof(ContainerId));
		memcpy(fingerprint, p, sizeof(Fingerprint));
		p += sizeof(Fingerprint);
		memcpy(cid, p, sizeof(ContainerId));
		p += sizeof(ContainerId);
		g_hash_table_insert(manifest->fingers, fingerprint, cid);
	}

	if ((p - buffer) != length)
		dprint("p - buffer != length");
}

int64_t write_manifest(Manifest *manifest) {
	int number = g_hash_table_size(manifest->fingers);
	int length = 8 + number * (sizeof(Fingerprint) + sizeof(ContainerId));
	int64_t id = (manifest_volume_length << 0x18) + length;
	/*	printf("write id = %lld, %lld, %d\n", id, manifest_volume_length, length);*/

	char buffer[length];
	char *p = buffer;

	*(int64_t*) p = id;
	p += 8;
	GHashTableIter iter;
	gpointer key, value;

	g_hash_table_iter_init(&iter, manifest->fingers);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		memcpy(p, key, sizeof(Fingerprint));
		p += sizeof(Fingerprint);
		memcpy(p, value, sizeof(ContainerId));
		p += sizeof(ContainerId);
	}

	if (p - buffer != length)
		dprint("it can't happen!");

	lseek(mvol_fd, manifest_volume_length, SEEK_SET);
	write(mvol_fd, buffer, length);

	manifest_volume_length += length;

	return id;
}

/*
 * indicates the current segment is completely duplicate with the top 1 champion.
 * If the current segment is duplicate with the top 1 champion,
 * it's unnecessary to add a new manifest to volume.
 * */
static BOOL completely_duplicate_with_top_1 = TRUE;

BOOL sparse_index_init() {
	sparse_index = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal,
			free, g_sequence_free);

	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/sparse_index");

	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open sparse_index");
		return FALSE;
	}

	struct stat fileinfo;
	stat(filename, &fileinfo);
	size_t filesize = fileinfo.st_size;

	if (filesize > 0) {
		int item_num = 0;
		read(fd, &item_num, 4);
		int i = 0;
		for (; i < item_num; ++i) {
			Fingerprint *finger = (Fingerprint*) malloc(sizeof(Fingerprint));
			GSequence *sequence = g_sequence_new(free);
			read(fd, finger, sizeof(Fingerprint));
			int mnum = 0;
			read(fd, &mnum, 4);
			int j = 0;
			for (; j < mnum; ++j) {
				int64_t *id = (int64_t*) malloc(sizeof(int64_t));
				read(fd, id, 8);
				g_sequence_append(sequence, id);
			}
			g_hash_table_insert(sparse_index, finger, sequence);
		}
	}
	close(fd);

	manifest_volume_init();

	return TRUE;
}

void sparse_index_destroy() {
	manifest_volume_destroy();

	char filename[256];
	strcpy(filename, working_path);
	strcat(filename, "index/sparse_index");

	int fd;
	if ((fd = open(filename, O_CREAT | O_RDWR, S_IRWXU)) < 0) {
		dprint("failed to open sparse_index");
		return;
	}

	int item_num = g_hash_table_size(sparse_index);
	write(fd, &item_num, 4);
	GHashTableIter iter;
	gpointer key, value;

	g_hash_table_iter_init(&iter, sparse_index);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		write(fd, key, sizeof(Fingerprint));
		GSequence *sequence = (GSequence*) value;
		int mnum = g_sequence_get_length(sequence);
		write(fd, &mnum, 4);
		GSequenceIter *s_iter = g_sequence_get_begin_iter(sequence);
		while (!g_sequence_iter_is_end(s_iter)) {
			int64_t *id = g_sequence_get(s_iter);
			write(fd, id, 8);
			s_iter = g_sequence_iter_next(s_iter);
		}
	}

	g_hash_table_destroy(sparse_index);

}

ContainerId sparse_index_search(Fingerprint *fingerprint,
		EigenValue *eigenvalue) {
	ContainerId ret_id = TMP_CONTAINER_ID;
	static int chunk_num = 0;
	static GSequence *champions = NULL;

	if (eigenvalue) {
		/* A new segment */
		if (chunk_num != 0 || champions != NULL )
			dprint("An error!");
		chunk_num = eigenvalue->chunk_num;
		champions = select_champions(eigenvalue);

		GSequenceIter *champion_iter = g_sequence_get_begin_iter(champions);
		while (!g_sequence_iter_is_end(champion_iter)) {
			Manifest *manifest = g_sequence_get(champion_iter);
			load_manifest(manifest);
			champion_iter = g_sequence_iter_next(champion_iter);
		}
	}

	ContainerId *cid = NULL;
	GSequenceIter *champion_iter = g_sequence_get_begin_iter(champions);
	while (!g_sequence_iter_is_end(champion_iter)) {
		if (!g_sequence_iter_is_begin(champion_iter))
			/* not in top 1 */
			completely_duplicate_with_top_1 = FALSE;
		Manifest *manifest = g_sequence_get(champion_iter);
		cid = g_hash_table_lookup(manifest->fingers, fingerprint);
		if (cid)
			break;
		champion_iter = g_sequence_iter_next(champion_iter);
	}

	if (cid == NULL && new_manifest) {
		cid = g_hash_table_lookup(new_manifest->fingers, fingerprint);
		completely_duplicate_with_top_1 = FALSE;
	}

	if (cid)
		ret_id = *cid;

	chunk_num--;
	if (chunk_num == 0) {
		g_sequence_free(champions);
		champions = NULL;
	}

	return ret_id;
}

/*
 * update is for rewriting.
 * We never know whether the container_id is updated without a update flag.
 * hooks param is obsolete.
 */
void sparse_index_update(Fingerprint *fingerprint, ContainerId container_id,
		EigenValue *eigenvalue, BOOL update) {
	static int chunk_num = 0;
	static EigenValue *current_eigenvalue = NULL;

	if (eigenvalue) {
		if (new_manifest != NULL )
			dprint("An error");
		chunk_num = eigenvalue->chunk_num;
		new_manifest = (Manifest*) malloc(sizeof(Manifest));
		new_manifest->matched_hooks = NULL;
		new_manifest->fingers = g_hash_table_new_full(g_int64_hash,
				g_fingerprint_equal, free, free);
		current_eigenvalue = (EigenValue*) malloc(
				sizeof(EigenValue)
						+ eigenvalue->value_num * sizeof(Fingerprint));
		memcpy(current_eigenvalue, eigenvalue,
				sizeof(EigenValue)
						+ eigenvalue->value_num * sizeof(Fingerprint));
	}

	if (update)
		completely_duplicate_with_top_1 = FALSE;

	Fingerprint *new_finger = (Fingerprint*) malloc(sizeof(Fingerprint));
	memcpy(new_finger, fingerprint, sizeof(Fingerprint));
	ContainerId *cid = (ContainerId*) malloc(sizeof(ContainerId));
	*cid = container_id;

	g_hash_table_insert(new_manifest->fingers, new_finger, cid);

	chunk_num--;
	if (chunk_num == 0) {
		/* The segment is finished, so write the manifest */
		if (completely_duplicate_with_top_1 == FALSE) {
			int64_t manifest_id = write_manifest(new_manifest);

			int i = 0;
			for (; i < current_eigenvalue->value_num; ++i) {
				GSequence *id_seq = g_hash_table_lookup(sparse_index,
						&current_eigenvalue->values[i]);
				if (id_seq == NULL ) {
					id_seq = g_sequence_new(free);
					Fingerprint *new_hook = (Fingerprint*) malloc(
							sizeof(Fingerprint));
					memcpy(new_hook, &current_eigenvalue->values[i],
							sizeof(Fingerprint));
					g_hash_table_insert(sparse_index, new_hook, id_seq);
				}
				int64_t *id = (int64_t*) malloc(sizeof(int64_t));
				*id = manifest_id;
				g_sequence_append(id_seq, id);
			}
		}

		free_manifest(new_manifest);
		new_manifest = NULL;

		free(current_eigenvalue);
		current_eigenvalue = NULL;
	}
}

EigenValue* extract_eigenvalue_sparse(Chunk* chunk) {
	static int chunk_cnt = 0;
	static Queue *current_hooks = NULL;
	if (current_hooks == NULL )
		current_hooks = queue_new();

	if (chunk->length == FILE_END)
		return NULL ;
	EigenValue *eigenvalue = NULL;
	if (chunk->length == STREAM_END || and_bits(chunk->hash, segment_bits)) {
		int cnt = queue_size(current_hooks);

		if (cnt > 0) {
			eigenvalue = (EigenValue*) malloc(
					sizeof(EigenValue) + cnt * sizeof(Fingerprint));
			eigenvalue->value_num = 0;
			int i = 0;
			for (; i < cnt; ++i) {
				Fingerprint *hook = queue_pop(current_hooks);
				memcpy(&eigenvalue->values[i], hook, sizeof(Fingerprint));
				eigenvalue->value_num++;
			}
			/*
			 * It is possible that the first segment in the file has no hooks.
			 * In this case (cnt == 0),
			 * we ignore the segment boundary,
			 * and merge it into next segment.
			 */
			eigenvalue->chunk_num = chunk_cnt;
			chunk_cnt = 0;
		}
	}

	if (chunk->length != STREAM_END) {
		chunk_cnt++;
		if (and_bits(chunk->hash, sample_bits)) {
			Fingerprint *new_hook = (Fingerprint*) malloc(sizeof(Fingerprint));
			memcpy(new_hook, &chunk->hash, sizeof(Fingerprint));
			queue_push(current_hooks, new_hook);
		}
	}

	return eigenvalue;
}
