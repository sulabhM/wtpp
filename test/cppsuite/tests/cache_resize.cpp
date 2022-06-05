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
#include "src/component/operation_tracker.h"
#include "src/main/test.h"

using namespace test_harness;

/* Defines what data is written to the tracking table for use in custom validation. */
class OperationTrackerCacheResize : public OperationTracker {

    public:
    OperationTrackerCacheResize(
      Configuration *config, const bool use_compression, TimestampManager &tsm)
        : OperationTracker(config, use_compression, tsm)
    {
    }

    void
    setTrackingCursor(const uint64_t txn_id, const trackingOperation &operation, const uint64_t &,
      const std::string &, const std::string &value, wt_timestamp_t ts,
      ScopedCursor &op_track_cursor) override final
    {
        op_track_cursor->set_key(op_track_cursor.Get(), ts, txn_id);
        op_track_cursor->set_value(op_track_cursor.Get(), operation, value.c_str());
    }
};

/*
 * This test continuously writes transactions larger than 1MB but less than 500MB into the database,
 * while switching the connection cache size between 1MB and 500MB. When transactions are larger
 * than the cache size they are rejected, so only transactions made when cache size is 500MB should
 * be allowed.
 */
class cache_resize : public Test {
    public:
    cache_resize(const test_args &args) : Test(args)
    {
        InitOperationTracker(
          new OperationTrackerCacheResize(_config->GetSubconfig(operationTracker),
            _config->GetBool(compressionEnabled), *_timestampManager));
    }

    void
    CustomOperation(thread_worker *tc) override final
    {
        WT_CONNECTION *conn = ConnectionManager::GetInstance().GetConnection();
        WT_CONNECTION_IMPL *conn_impl = (WT_CONNECTION_IMPL *)conn;
        bool increase_cache = false;
        const std::string small_cache_size = "cache_size=1MB";
        const std::string big_cache_size = "cache_size=500MB";

        while (tc->running()) {
            tc->sleep();

            /* Get the current cache size. */
            uint64_t prev_cache_size = conn_impl->cache_size;

            /* Reconfigure with the new cache size. */
            testutil_check(conn->reconfigure(
              conn, increase_cache ? big_cache_size.c_str() : small_cache_size.c_str()));

            /* Get the new cache size. */
            uint64_t new_cache_size = conn_impl->cache_size;

            Logger::LogMessage(LOG_TRACE,
              "The cache size was updated from " + std::to_string(prev_cache_size) + " to " +
                std::to_string(new_cache_size));

            /*
             * The collection id and the key are dummy fields which are required by the
             * save_operation API but not needed for this test.
             */
            const uint64_t collection_id = 0;
            const std::string key;
            const std::string value = std::to_string(new_cache_size);

            /* Retrieve the current transaction id. */
            uint64_t txn_id = ((WT_SESSION_IMPL *)tc->session.Get())->txn->id;

            /* Save the change of cache size in the tracking table. */
            tc->txn.Start();
            int ret = tc->op_tracker->save_operation(txn_id, trackingOperation::CUSTOM,
              collection_id, key, value, tc->tsm->GetNextTimestamp(), tc->op_track_cursor);

            if (ret == 0)
                testutil_assert(tc->txn.Commit());
            else {
                /* Due to the cache pressure, it is possible to fail when saving the operation. */
                testutil_assert(ret == WT_ROLLBACK);
                Logger::LogMessage(LOG_WARN,
                  "The cache size reconfiguration could not be saved in the tracking table, ret: " +
                    std::to_string(ret));
                tc->txn.Rollback();
            }
            increase_cache = !increase_cache;
        }
    }

    void
    InsertOperation(thread_worker *tc) override final
    {
        const uint64_t collection_count = tc->db.GetCollectionCount();
        testutil_assert(collection_count > 0);
        Collection &coll = tc->db.GetCollection(collection_count - 1);
        ScopedCursor cursor = tc->session.OpenScopedCursor(coll.name);

        while (tc->running()) {
            tc->sleep();

            /* Insert the current cache size value using a random key. */
            const std::string key =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(tc->key_size);
            const uint64_t cache_size =
              ((WT_CONNECTION_IMPL *)ConnectionManager::GetInstance().GetConnection())->cache_size;
            /* Take into account the value size given in the test configuration file. */
            const std::string value = std::to_string(cache_size);

            tc->txn.TryStart();
            if (!tc->insert(cursor, coll.id, key, value)) {
                tc->txn.Rollback();
            } else if (tc->txn.CanCommit()) {
                /*
                 * The transaction can fit in the current cache size and is ready to be committed.
                 * This means the tracking table will contain a new record to represent this
                 * transaction which will be used during the validation stage.
                 */
                testutil_assert(tc->txn.Commit());
            }
        }

        /* Make sure the last transaction is rolled back now the work is finished. */
        if (tc->txn.Active())
            tc->txn.Rollback();
    }

    void
    Validate(const std::string &operation_table_name, const std::string &,
      const std::vector<uint64_t> &) override final
    {
        bool first_record = false;
        int ret;
        uint64_t cache_size, num_records = 0, prev_txn_id;
        const uint64_t cache_size_500mb = 500000000;

        /* FIXME-WT-9339. */
        (void)cache_size;
        (void)cache_size_500mb;

        /* Open a cursor on the tracking table to read it. */
        ScopedSession session = ConnectionManager::GetInstance().CreateSession();
        ScopedCursor cursor = session.OpenScopedCursor(operation_table_name);

        /*
         * Parse the tracking table. Each operation is tracked and each transaction is made of
         * multiple operations, hence we expect multiple records for each transaction. We only need
         * to verify that the cache size was big enough when the transaction was committed, which
         * means at the last operation.
         */
        while ((ret = cursor->next(cursor.Get())) == 0) {

            uint64_t tracked_ts, tracked_txn_id;
            int tracked_op_type;
            const char *tracked_cache_size;

            testutil_check(cursor->get_key(cursor.Get(), &tracked_ts, &tracked_txn_id));
            testutil_check(cursor->get_value(cursor.Get(), &tracked_op_type, &tracked_cache_size));

            Logger::LogMessage(LOG_TRACE,
              "Timestamp: " + std::to_string(tracked_ts) +
                ", transaction id: " + std::to_string(tracked_txn_id) +
                ", cache size: " + std::to_string(std::stoull(tracked_cache_size)));

            trackingOperation op_type = static_cast<trackingOperation>(tracked_op_type);
            /* There are only two types of operation tracked. */
            testutil_assert(
              op_type == trackingOperation::CUSTOM || op_type == trackingOperation::INSERT);

            /*
             * There is nothing to do if we are reading a record that indicates a cache size change.
             */
            if (op_type == trackingOperation::CUSTOM)
                continue;

            if (first_record) {
                first_record = false;
            } else if (prev_txn_id != tracked_txn_id) {
                /*
                 * We have moved to a new transaction, make sure the cache was big enough when the
                 * previous transaction was committed.
                 *
                 * FIXME-WT-9339 - Somehow we have some transactions that go through while the cache
                 * is very low. Enable the check when this is no longer the case.
                 *
                 * testutil_assert(cache_size > cache_size_500mb);
                 */
            }
            prev_txn_id = tracked_txn_id;
            /* Save the last cache size seen by the transaction. */
            cache_size = std::stoull(tracked_cache_size);
            ++num_records;
        }
        /* All records have been parsed, the last one still needs the be checked. */
        testutil_assert(ret == WT_NOTFOUND);
        testutil_assert(num_records > 0);
        /*
         * FIXME-WT-9339 - Somehow we have some transactions that go through while the cache is very
         * low. Enable the check when this is no longer the case.
         *
         * testutil_assert(cache_size > cache_size_500mb);
         */
    }
};
