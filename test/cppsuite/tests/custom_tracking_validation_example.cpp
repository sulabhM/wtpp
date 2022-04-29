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

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class custom_tracking_validation_example : public test_harness::test {
    public:
    custom_tracking_validation_example(const test_harness::test_args &args) : test(args) {}

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

    void
    custom_operation(test_harness::thread_context *tc) override final
    {
        bool change = false;
        uint64_t cache_size;
        while (tc->running()) {
            tc->sleep();
            WT_CONNECTION_IMPL *conn = S2C((WT_SESSION_IMPL *)tc->session.get());
            cache_size = conn->cache_size;
            // Need to reconfigure the cache size
            WT_CONNECTION *conn1 = (WT_CONNECTION *)conn;
            if (change) {
                testutil_check(conn1->reconfigure(conn1, "cache_size=10MB"));
            } else {
                testutil_check(conn1->reconfigure(conn1, "cache_size=500MB"));
            }
            std::cout << "Cache size was " << cache_size << " and is now " << conn->cache_size
                      << std::endl;
            change = !change;
        }
    }

    void
    insert_operation(test_harness::thread_context *tc) override final
    {
        // Open cursor on the collection
        scoped_cursor cursor = tc->session.open_scoped_cursor("table:collection_0");

        while (tc->running()) {
            tc->transaction.try_begin();
            std::cout << "Trying to insert data with key_size " << tc->key_size
                      << " and value_size " << tc->value_size << std::endl;
            bool ret = tc->insert(cursor, 0, tc->id);

            if (!ret) {
                std::cout << "Need to rollback txn "
                          << ((WT_SESSION_IMPL *)tc->session.get())->txn->id << std::endl;
                tc->transaction.rollback();
            } else {
                if (tc->transaction.can_commit()) {
                    testutil_assert(tc->transaction.commit() == true);
                    std::cout << "Commit done for txn "
                              << ((WT_SESSION_IMPL *)tc->session.get())->txn->id << std::endl;
                } else {
                    std::cout << "Cannot commit yet" << std::endl;
                }
            }

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
    validate(const std::string &, const std::string &, const std::vector<uint64_t> &) override final
    {
        std::cout << "validate: nothing done." << std::endl;
    }
};
