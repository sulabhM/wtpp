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

#include "test_harness/test.h"

namespace test_harness {

/* Defines what data is written to the tracking table for use in custom validation. */
class tracking_table_template_1 : public test_harness::workload_tracking {

    public:
    tracking_table_template_1(
      configuration *config, const bool use_compression, timestamp_manager &tsm)
        : workload_tracking(config, use_compression, tsm)
    {
    }

    void
    set_tracking_cursor(scoped_session &tc_session, const tracking_operation &operation,
      const uint64_t &collection_id, const std::string &key, const std::string &value,
      wt_timestamp_t ts, scoped_cursor &op_track_cursor) override final
    {
        // std::cout << "set_tracking_cursor - txn id: "
        //           << ((WT_SESSION_IMPL *)tc_session.get())->txn->id << std::endl;
        // std::cout << "set_tracking_cursor - ts: " << ts << std::endl;
        WT_CONNECTION_IMPL *conn = S2C((WT_SESSION_IMPL *)tc_session.get());
        uint64_t cache_size = conn->cache_size;
        uint64_t txn_id = ((WT_SESSION_IMPL *)tc_session.get())->txn->id;
        // std::cout << "set_tracking_cursor - cache: " << cache_size << std::endl;

        /* You can replace this call to define your own tracking table contents. */
        op_track_cursor->set_key(op_track_cursor.get(), collection_id, key.c_str(), ts);
        op_track_cursor->set_value(op_track_cursor.get(), cache_size, txn_id);
        // workload_tracking::set_tracking_cursor(
        //   tc_session, operation, collection_id, key, value, ts, op_track_cursor);
    }
};
} // namespace test_harness

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class custom_tracking_validation_example : public test_harness::test {
    public:
    custom_tracking_validation_example(const test_harness::test_args &args) : test(args)
    {
        delete this->_workload_tracking;
        this->_workload_tracking =
          new tracking_table_template_1(_config->get_subconfig(WORKLOAD_TRACKING),
            _config->get_bool(COMPRESSION_ENABLED), *_timestamp_manager);
    }

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customized your test. */
        test::run();
    }

    // void
    // populate(test_harness::database &, test_harness::timestamp_manager *,
    //   test_harness::configuration *, test_harness::workload_tracking *) override final
    // {
    //     std::cout << "populate: nothing done." << std::endl;
    // }

    bool reconfigure_required = false;

    void
    custom_operation(test_harness::thread_context *tc) override final
    {
        bool change = false;
        uint64_t cache_size;
        while (tc->running()) {
            tc->sleep();
            // The system has to stop here when we reconfigure
            reconfigure_required = true;
            while(inserts_running != 0 && tc->running()) {
                std::cout << "We want to reconfigure but inserts are running..." << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            WT_CONNECTION_IMPL *conn = S2C((WT_SESSION_IMPL *)tc->session.get());
            cache_size = conn->cache_size;
            // Need to reconfigure the cache size
            WT_CONNECTION *conn1 = (WT_CONNECTION *)conn;
            if (change) {
                testutil_check(conn1->reconfigure(conn1, "cache_size=1MB"));
            } else {
                testutil_check(conn1->reconfigure(conn1, "cache_size=500MB"));
            }
            std::cout << "Cache size was " << cache_size << " and is now " << conn->cache_size
                      << std::endl;
            change = !change;
            reconfigure_required = false;
        }
    }

uint64_t inserts_running = 0;
    void
    insert_operation(test_harness::thread_context *tc) override final
    {
        // Open cursor on the collection
        scoped_cursor cursor = tc->session.open_scoped_cursor("table:collection_0");

        while (tc->running()) {
            ++inserts_running;
            std::cout << "Starting inserts..." << std::endl;
            tc->transaction.try_begin();
            // std::cout << "Trying to insert data with key_size " << tc->key_size
            //           << " and value_size " << tc->value_size << std::endl;
            const std::string value = random_generator::instance().generate_pseudo_random_string(tc->value_size);
            bool ret = tc->insert(cursor, 0, value);

            if(reconfigure_required) {
                std::cout << "reconfiguration is required, aborting..." << std::endl;
            }
            if (!ret || reconfigure_required) {
                std::cout << "Need to rollback txn "
                          << ((WT_SESSION_IMPL *)tc->session.get())->txn->id << std::endl;
                tc->transaction.rollback();
            } else {
                if (tc->transaction.can_commit()) {
                    testutil_assert(tc->transaction.commit() == true);
                    std::cout << "Commit done for txn "
                              << ((WT_SESSION_IMPL *)tc->session.get())->txn->id << std::endl;
                } 
                // else {
                //     std::cout << "Cannot commit yet" << std::endl;
                // }
            }
            --inserts_running;
            std::cout << "Sleeping..." << std::endl;
            tc->sleep();
        }
        /* Make sure the last transaction is rolled back now the work is finished. */
        if (tc->transaction.active())
            tc->transaction.rollback();
    }

    void
    read_operation(test_harness::thread_context *) override final
    {
        std::cout << "read_operation: nothing done." << std::endl;
    }

    void
    remove_operation(test_harness::thread_context *) override final
    {
        std::cout << "remove_operation: nothing done." << std::endl;
    }

    void
    update_operation(test_harness::thread_context *) override final
    {
        std::cout << "update_operation: nothing done." << std::endl;
    }

    void
    validate(const std::string &operation_table_name, const std::string &,
      const std::vector<uint64_t> &) override final
    {
        WT_DECL_RET;
        uint64_t tracked_collection_id, tracked_cache_size, tracked_txn_id;
        wt_timestamp_t tracked_timestamp;
        const char *tracked_key;

        // I need to read the tracking table and check the keys and values.
        std::cout << "validate: Trying custom..." << std::endl;
        scoped_session session = connection_manager::instance().create_session();
        scoped_cursor cursor = session.open_scoped_cursor(operation_table_name);

        int cpt = 0;
        while ((ret = cursor->next(cursor.get())) == 0) {
            ++cpt;
            testutil_check(cursor->get_key(
              cursor.get(), &tracked_collection_id, &tracked_key, &tracked_timestamp));
            testutil_check(cursor->get_value(cursor.get(), &tracked_cache_size, &tracked_txn_id));
            // std::cout << "tracked_collection_id: " << tracked_collection_id << std::endl;
            // std::cout << "tracked_key: " << tracked_key << std::endl;
            // std::cout << "tracked_timestamp: " << tracked_timestamp << std::endl;
            std::cout << "tracked_txn_id: " << tracked_txn_id << " - tracked_cache_size: " << tracked_cache_size << std::endl;
            /* Transactions could go through only when the cache size was large enough. */
            // testutil_assert(tracked_cache_size >= 524288000);
        }
        std::cout << "cpt is " << cpt << std::endl;
        /* Four records had time to go through. */
        // testutil_assert(cpt == 4);
    }
};
