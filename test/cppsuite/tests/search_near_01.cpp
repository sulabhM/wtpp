/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "src/common/constants.h"
#include "src/common/random_generator.h"
#include "src/main/test.h"

using namespace test_harness;

/*
 * In this test, we want to verify that search_near with prefix enabled only traverses the portion
 * of the tree that follows the prefix portion of the search key. The test is composed of a populate
 * phase followed by a read phase. The populate phase will insert a set of random generated keys
 * with a prefix of aaa -> zzz. During the read phase, we have one read thread that performs:
 *  - Spawning multiple threads to perform one prefix search near.
 *  - Waiting on all threads to finish.
 *  - Using WiredTiger statistics to validate that the number of entries traversed is within
 * bounds of the search key.
 */
class search_near_01 : public Test {
    uint64_t keys_per_prefix = 0;
    uint64_t srchkey_len = 0;
    const std::string ALPHABET{"abcdefghijklmnopqrstuvwxyz"};
    const uint64_t PREFIX_KEY_LEN = 3;
    const int64_t MINIMUM_EXPECTED_ENTRIES = 40;

    static void
    populate_worker(thread_worker *tc, const std::string &ALPHABET, uint64_t PREFIX_KEY_LEN)
    {
        Logger::LogMessage(LOG_INFO, "Populate with thread id: " + std::to_string(tc->id));

        uint64_t collections_per_thread = tc->collection_count;
        const uint64_t MAX_ROLLBACKS = 100;
        uint32_t rollback_retries = 0;

        /*
         * Generate a table of data with prefix keys aaa -> zzz. We have 26 threads from ids
         * starting from 0 to 26. Each populate thread will insert separate prefix keys based on the
         * id.
         */
        for (int64_t i = 0; i < collections_per_thread; ++i) {
            Collection &coll = tc->db.GetCollection(i);
            ScopedCursor cursor = tc->session.OpenScopedCursor(coll.name);
            for (uint64_t j = 0; j < ALPHABET.size(); ++j) {
                for (uint64_t k = 0; k < ALPHABET.size(); ++k) {
                    for (uint64_t count = 0; count < tc->key_count; ++count) {
                        tc->txn.Start();
                        /*
                         * Generate the prefix key, and append a random generated key string based
                         * on the key size configuration.
                         */
                        std::string prefix_key = {
                          ALPHABET.at(tc->id), ALPHABET.at(j), ALPHABET.at(k)};
                        prefix_key += RandomGenerator::GetInstance().GenerateRandomString(
                          tc->key_size - PREFIX_KEY_LEN);
                        std::string value =
                          RandomGenerator::GetInstance().GeneratePseudoRandomString(tc->value_size);
                        if (!tc->insert(cursor, coll.id, prefix_key, value)) {
                            testutil_assert(rollback_retries < MAX_ROLLBACKS);
                            /* We failed to insert, rollback our transaction and retry. */
                            tc->txn.Rollback();
                            ++rollback_retries;
                            if (count > 0)
                                --count;
                        } else {
                            /* Commit txn at commit timestamp 100. */
                            testutil_assert(
                              tc->txn.Commit("commit_timestamp=" + tc->tsm->DecimalToHex(100)));
                            rollback_retries = 0;
                        }
                    }
                }
            }
        }
    }

    public:
    search_near_01(const test_args &args) : Test(args)
    {
        InitOperationTracker();
    }

    void
    Populate(Database &database, TimestampManager *tsm, Configuration *config,
      OperationTracker *op_tracker) override final
    {
        uint64_t collection_count, key_size;
        std::vector<thread_worker *> workers;
        ThreadManager tm;

        /* Validate our config. */
        collection_count = config->GetInt(collectionCount);
        keys_per_prefix = config->GetInt(keyCountPerCollection);
        key_size = config->GetInt(keySize);
        testutil_assert(collection_count > 0);
        testutil_assert(keys_per_prefix > 0);
        /* Check the prefix length is not greater than the key size. */
        testutil_assert(key_size >= PREFIX_KEY_LEN);

        Logger::LogMessage(LOG_INFO,
          "Populate configuration with key size: " + std::to_string(key_size) +
            " key count: " + std::to_string(keys_per_prefix) +
            " number of collections: " + std::to_string(collection_count));

        /* Create n collections as per the configuration. */
        for (uint64_t i = 0; i < collection_count; ++i)
            /*
             * The database model will call into the API and create the collection, with its own
             * session.
             */
            database.AddCollection();

        /* Spawn 26 threads to populate the database. */
        for (uint64_t i = 0; i < ALPHABET.size(); ++i) {
            thread_worker *tc = new thread_worker(i, thread_type::INSERT, config,
              ConnectionManager::GetInstance().CreateSession(), tsm, op_tracker, database);
            workers.push_back(tc);
            tm.addThread(populate_worker, tc, ALPHABET, PREFIX_KEY_LEN);
        }

        /* Wait for our populate threads to finish and then join them. */
        Logger::LogMessage(LOG_INFO, "Populate: waiting for threads to complete.");
        tm.Join();

        /* Cleanup our workers. */
        for (auto &it : workers) {
            delete it;
            it = nullptr;
        }

        /* Force evict all the populated keys in all of the collections. */
        int cmpp;
        ScopedSession session = ConnectionManager::GetInstance().CreateSession();
        for (uint64_t count = 0; count < collection_count; ++count) {
            Collection &coll = database.GetCollection(count);
            ScopedCursor evict_cursor =
              session.OpenScopedCursor(coll.name.c_str(), "debug=(release_evict=true)");

            for (uint64_t i = 0; i < ALPHABET.size(); ++i) {
                for (uint64_t j = 0; j < ALPHABET.size(); ++j) {
                    for (uint64_t k = 0; k < ALPHABET.size(); ++k) {
                        std::string key = {ALPHABET.at(i), ALPHABET.at(j), ALPHABET.at(k)};
                        evict_cursor->set_key(evict_cursor.Get(), key.c_str());
                        evict_cursor->search_near(evict_cursor.Get(), &cmpp);
                        testutil_check(evict_cursor->reset(evict_cursor.Get()));
                    }
                }
            }
        }
        srchkey_len =
          RandomGenerator::GetInstance().GenerateInteger(static_cast<uint64_t>(1), PREFIX_KEY_LEN);
        Logger::LogMessage(LOG_INFO, "Populate: finished.");
    }

    static void
    perform_search_near(thread_worker *tc, std::string collection_name, uint64_t srchkey_len,
      std::atomic<int64_t> &z_key_searches)
    {
        std::string srch_key;
        int cmpp = 0;

        ScopedCursor cursor = tc->session.OpenScopedCursor(collection_name);
        cursor->reconfigure(cursor.Get(), "prefix_search=true");
        /* Generate search prefix key of random length between a -> zzz. */
        srch_key = RandomGenerator::GetInstance().GenerateRandomString(
          srchkey_len, charactersType::ALPHABET);
        Logger::LogMessage(LOG_TRACE,
          "Search near thread {" + std::to_string(tc->id) +
            "} performing prefix search near with key: " + srch_key);

        /*
         * Read at timestamp 10, so that no keys are visible to this transaction. When performing
         * prefix search near, we expect the search to early exit out of its prefix range and return
         * WT_NOTFOUND.
         */
        tc->txn.Start("read_timestamp=" + tc->tsm->DecimalToHex(10));
        if (tc->txn.Active()) {
            cursor->set_key(cursor.Get(), srch_key.c_str());
            testutil_assert(cursor->search_near(cursor.Get(), &cmpp) == WT_NOTFOUND);
            tc->txn.IncrementOp();

            /*
             * There is an edge case where we may not early exit the prefix search near call because
             * the specified prefix matches the rest of the entries in the tree.
             *
             * In this test, the keys in our database start with prefixes aaa -> zzz. If we search
             * with a prefix such as "z", we will not early exit the search near call because the
             * rest of the keys will also start with "z" and match the prefix. The statistic will
             * stay the same if we do not early exit search near, track this through incrementing
             * the number of z key searches we have done this iteration.
             */
            if (srch_key == "z" || srch_key == "zz" || srch_key == "zzz")
                ++z_key_searches;
            tc->txn.Rollback();
        }
    }

    void
    ReadOperation(thread_worker *tc) override final
    {
        /* Make sure that thread statistics cursor is null before we open it. */
        testutil_assert(tc->stat_cursor.Get() == nullptr);
        /* This test will only work with one read thread. */
        testutil_assert(tc->thread_count == 1);
        Configuration *workload_config, *read_config;
        std::vector<thread_worker *> workers;
        std::atomic<int64_t> z_key_searches;
        int64_t entries_stat, expected_entries, prefix_stat, prev_entries_stat, prev_prefix_stat;
        int num_threads;

        prev_entries_stat = 0;
        prev_prefix_stat = 0;
        num_threads = _config->GetInt("search_near_threads");
        tc->stat_cursor = tc->session.OpenScopedCursor(statisticsURI);
        workload_config = _config->GetSubconfig(workloadManager);
        read_config = workload_config->GetSubconfig(readOpConfig);
        z_key_searches = 0;

        Logger::LogMessage(LOG_INFO,
          type_string(tc->type) + " thread commencing. Spawning " + std::to_string(num_threads) +
            " search near threads.");

        /*
         * The number of expected entries is calculated to account for the maximum allowed entries
         * per search near function call. The key we search near can be different in length, which
         * will increase the number of entries search by a factor of 26.
         */
        expected_entries = keys_per_prefix * pow(ALPHABET.size(), PREFIX_KEY_LEN - srchkey_len);
        while (tc->running()) {
            MetricsMonitor::GetStatistics(
              tc->stat_cursor, WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100, &prev_entries_stat);
            MetricsMonitor::GetStatistics(tc->stat_cursor,
              WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS, &prev_prefix_stat);

            ThreadManager tm;
            for (uint64_t i = 0; i < num_threads; ++i) {
                /* Get a collection and find a cached cursor. */
                Collection &coll = tc->db.GetRandomCollection();
                thread_worker *search_near_tc = new thread_worker(i, thread_type::READ, read_config,
                  ConnectionManager::GetInstance().CreateSession(), tc->tsm, tc->op_tracker,
                  tc->db);
                workers.push_back(search_near_tc);
                tm.addThread(perform_search_near, search_near_tc, coll.name, srchkey_len,
                  std::ref(z_key_searches));
            }

            tm.Join();

            /* Cleanup our workers. */
            for (auto &it : workers) {
                delete it;
                it = nullptr;
            }
            workers.clear();

            MetricsMonitor::GetStatistics(
              tc->stat_cursor, WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100, &entries_stat);
            MetricsMonitor::GetStatistics(
              tc->stat_cursor, WT_STAT_CONN_CURSOR_SEARCH_NEAR_PREFIX_FAST_PATHS, &prefix_stat);
            Logger::LogMessage(LOG_TRACE,
              "Read thread skipped entries: " + std::to_string(entries_stat - prev_entries_stat) +
                " prefix early exit: " +
                std::to_string(prefix_stat - prev_prefix_stat - z_key_searches));
            /*
             * It is possible that WiredTiger increments the entries skipped stat irrelevant to
             * prefix search near. This is dependent on how many read threads are present in the
             * test. Account for this by creating a small buffer using thread count. Assert that the
             * number of expected entries is the upper limit which the prefix search near can
             * traverse.
             *
             * Assert that the number of expected entries is the maximum allowed limit that the
             * prefix search nears can traverse and that the prefix fast path has increased by the
             * number of threads minus the number of search nears with z key.
             */
            testutil_assert(num_threads * expected_entries + (2 * num_threads) >=
              entries_stat - prev_entries_stat);
            testutil_assert(prefix_stat - prev_prefix_stat == num_threads - z_key_searches);
            z_key_searches = 0;
            tc->sleep();
        }
        delete read_config;
        delete workload_config;
    }
};
