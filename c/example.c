#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <libwildcat.h>

/* create_directory
 * Helper function to create directory if it doesn't exist */
int
create_directory(path)
    const char *path;
{
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        if (mkdir(path, 0755) == -1) {
            perror("mkdir");
            return -1;
        }
    }
    return 0;
}

/* cleanup_directory
 * Helper function to cleanup directory */
void
cleanup_directory(path)
    const char *path;
{
    char command[512];
    snprintf(command, sizeof(command), "rm -rf %s", path);
    system(command);
}

int
main()
{
    printf(" \\ \\      / _ _|  |     _ \\   __|    \\ __ __| \n");
    printf("  \\ \\ \\  /    |   |     |  | (      _ \\   |   \n");
    printf("   \\_/\\_/   ___| ____| ___/ \\___| _/  _\\ _|   \n");
    printf("                                              \n");

    const char *db_path = "/tmp/wildcat_c_example/";

    /* Cleanup any existing directory */
    cleanup_directory(db_path);

    /* Create directory */
    if (create_directory(db_path) != 0) {
        fprintf(stderr, "Failed to create database directory\n");
        return 1;
    }

    /* Initialize options */
    wildcat_opts_t opts = {0};
    opts.directory = (char*)db_path;
    opts.write_buffer_size = 32 * 1024 * 1024;  /* 32MB */
    opts.sync_option = SYNC_ALWAYS;
    opts.sync_interval_ns = 0;
    opts.level_count = 6;
    opts.level_multiplier = 10;
    opts.block_manager_lru_size = 256;
    opts.block_manager_lru_evict_ratio = 0.2;
    opts.block_manager_lru_access_weight = 0.8;
    opts.permission = 0755;
    opts.bloom_filter = 0;  /* false */
    opts.max_compaction_concurrency = 4;
    opts.compaction_cooldown_ns = 5000000000LL;  /* 5 seconds */
    opts.compaction_batch_size = 8;
    opts.compaction_size_ratio = 1.1;
    opts.compaction_size_threshold = 8;
    opts.compaction_score_size_weight = 0.8;
    opts.compaction_score_count_weight = 0.2;
    opts.flusher_interval_ns = 1000000LL;  /* 1ms */
    opts.compactor_interval_ns = 250000000LL;  /* 250ms */
    opts.bloom_fpr = 0.01;
    opts.wal_append_retry = 10;
    opts.wal_append_backoff_ns = 128000LL;  /* 128µs */
    opts.sstable_btree_order = 10;

    /* Open database */
    printf("Opening Wildcat database...\n");
    unsigned long db_handle = wildcat_open(&opts);
    if (db_handle == 0) {
        fprintf(stderr, "Failed to open database\n");
        cleanup_directory(db_path);
        return 1;
    }
    printf("Database opened successfully (handle: 0x%lx)\n\n", db_handle);

    /* === Basic Put/Get Operations === */
    printf("=== Basic Put/Get Operations ===\n");
    long txn_id = wildcat_begin_txn(db_handle);
    if (txn_id == -1) {
        fprintf(stderr, "Failed to begin transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Transaction started (ID: %ld)\n", txn_id);

    /* Put operations */
    if (wildcat_txn_put(txn_id, "hello", "world") != 0) {
        fprintf(stderr, "Failed to put hello->world\n");
        (void)wildcat_txn_rollback(txn_id);
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Put: hello -> world\n");

    if (wildcat_txn_put(txn_id, "foo", "bar") != 0) {
        fprintf(stderr, "Failed to put foo->bar\n");
        (void)wildcat_txn_rollback(txn_id);
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Put: foo -> bar\n");

    /* Commit transaction */
    if (wildcat_txn_commit(txn_id) != 0) {
        fprintf(stderr, "Failed to commit transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Transaction committed\n\n");

    /* === Reading Data === */
    printf("=== Reading Data ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id == -1) {
        fprintf(stderr, "Failed to begin read transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    char *value = wildcat_txn_get(txn_id, "hello");
    if (value) {
        printf("Get: hello -> %s\n", value);
        free(value);  /* Don't forget to free the returned string */
    } else {
        printf("Get: hello -> NOT FOUND\n");
    }

    value = wildcat_txn_get(txn_id, "foo");
    if (value) {
        printf("Get: foo -> %s\n", value);
        free(value);
    } else {
        printf("Get: foo -> NOT FOUND\n");
    }

    (void)wildcat_txn_rollback(txn_id);  /* Read-only transaction, just rollback */
    printf("\n");

    /* === Batch Operations === */
    printf("=== Batch Operations ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id == -1) {
        fprintf(stderr, "Failed to begin batch transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    /* Insert multiple key-value pairs */
    {
        int i;
        for (i = 0; i < 10; i++) {
            char key[16], val[32];
            snprintf(key, sizeof(key), "key%d", i);
            snprintf(val, sizeof(val), "value%d", i);

            if (wildcat_txn_put(txn_id, key, val) != 0) {
                fprintf(stderr, "Failed to put %s->%s\n", key, val);
                wildcat_txn_rollback(txn_id);
                (void)wildcat_close(db_handle);
                (void)cleanup_directory(db_path);
                return 1;
            }
            printf("Put: %s -> %s\n", key, val);
        }
    }

    if (wildcat_txn_commit(txn_id) != 0) {
        fprintf(stderr, "Failed to commit batch transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Batch transaction committed\n\n");

    /* === Iterator Example === */
    printf("=== Iterator Example ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id == -1) {
        fprintf(stderr, "Failed to begin iterator transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    /* Create iterator (ascending=1) */
    unsigned long iter_id = wildcat_txn_new_iterator(txn_id, 1);
    if (iter_id == 0) {
        fprintf(stderr, "Failed to create iterator\n");
        (void)wildcat_txn_rollback(txn_id);
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    printf("Iterating through all keys:\n");
    /* The iterator is already positioned at the first element after creation */
    /* Check if it's valid and iterate */
    if (wildcat_txn_iter_valid(iter_id)) {
        do {
            char *key = wildcat_iterator_key(iter_id);
            char *val = wildcat_iterator_value(iter_id);

            if (key && val) {
                printf("  %s -> %s\n", key, val);
                free(key);
                free(val);
            }
        } while (wildcat_txn_iterate_next(iter_id) == 0);  /* 0 means valid/success */
    }

    (void)wildcat_iterator_free(iter_id);
    (void)wildcat_txn_rollback(txn_id);
    printf("\n");

    /* === Delete Operation === */
    printf("=== Delete Operation ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id == -1) {
        fprintf(stderr, "Failed to begin delete transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    if (wildcat_txn_delete(txn_id, "key5") != 0) {
        fprintf(stderr, "Failed to delete key5\n");
        wildcat_txn_rollback(txn_id);
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }
    printf("Deleted key: key5\n");

    if (wildcat_txn_commit(txn_id) != 0) {
        fprintf(stderr, "Failed to commit delete transaction\n");
        (void)wildcat_close(db_handle);
        (void)cleanup_directory(db_path);
        return 1;
    }

    /* === Verify Deletion === */
    printf("=== Verify Deletion ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id != -1) {
        value = wildcat_txn_get(txn_id, "key5");
        if (value == NULL) {
            printf("SUCCESS: key5 was successfully deleted\n");
        } else {
            printf("ERROR: key5 still exists with value: %s\n", value);
            free(value);
        }
        (void)wildcat_txn_rollback(txn_id);
    }
    printf("\n");

    /* === Force Flush === */
    printf("=== Force Flush ===\n");
    if (wildcat_force_flush(db_handle) == 0) {
        printf("Force flush completed successfully\n");
    } else {
        printf("Force flush failed\n");
    }
    printf("\n");

    /* === Database Statistics === */
    printf("=== Database Statistics ===\n");
    {
        char *stats = wildcat_stats(db_handle);
        if (stats) {
            printf("%s\n", stats);
            free(stats);
        } else {
            printf("Failed to get database statistics\n");
        }
    }

    /* === Range Iterator Example === */
    printf("=== Range Iterator Example ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id != -1) {
        /* Create range iterator from "key0" to "key5" (exclusive) */
        unsigned long range_iter_id = wildcat_txn_new_range_iterator(txn_id, "key0", "key5", 1);
        if (range_iter_id != 0) {
            printf("Iterating through range [key0, key5):\n");

            do {
                char *key = wildcat_iterator_key(range_iter_id);
                char *val = wildcat_iterator_value(range_iter_id);

                if (key && val) {
                    printf("  %s -> %s\n", key, val);
                    free(key);
                    free(val);
                }
            } while (wildcat_txn_iterate_next(range_iter_id) == 0);

            (void)wildcat_iterator_free(range_iter_id);
        } else {
            printf("Failed to create range iterator\n");
        }
        (void)wildcat_txn_rollback(txn_id);
    }
    printf("\n");

    /* === Prefix Iterator Example === */
    printf("=== Prefix Iterator Example ===\n");
    txn_id = wildcat_begin_txn(db_handle);
    if (txn_id != -1) {
        /* Create prefix iterator for keys starting with "key" */
        unsigned long prefix_iter_id = wildcat_txn_new_prefix_iterator(txn_id, "key", 1);
        if (prefix_iter_id != 0) {
            printf("Iterating through keys with prefix 'key':\n");

            do {
                char *key = wildcat_iterator_key(prefix_iter_id);
                char *val = wildcat_iterator_value(prefix_iter_id);

                if (key && val) {
                    printf("  %s -> %s\n", key, val);
                    free(key);
                    free(val);
                }
            } while (wildcat_txn_iterate_next(prefix_iter_id) == 0);

            (void)wildcat_iterator_free(prefix_iter_id);
        } else {
            printf("Failed to create prefix iterator\n");
        }
        (void)wildcat_txn_rollback(txn_id);
    }

    /* Close database instance */
    printf("Closing database...\n");
    (void)wildcat_close(db_handle);
    printf("Database closed successfully\n");

    /* Cleanup */
    (void)cleanup_directory(db_path);

    return 0;
}