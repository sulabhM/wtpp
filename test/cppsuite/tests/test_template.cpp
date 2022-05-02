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

/* Class that specifies what to write into the tracking table. */
class tracking_table_template : public test_harness::workload_tracking {

    using workload_tracking::workload_tracking;

    int
    populate_tracking_cursor(const tracking_operation &operation, const uint64_t &collection_id,
      const char *key, const char *value, wt_timestamp_t ts,
      scoped_cursor &op_track_cursor) override final
    {
        /* You can replace this call to define your own tracking table contents. */
        workload_tracking::populate_tracking_cursor(
          operation, collection_id, key, value, ts, op_track_cursor);
    }
};

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class test_template : public test_harness::test {
    public:
    test_template(const test_harness::test_args &args) : test(args)
    {
        delete this->_workload_tracking;
        this->_workload_tracking =
          new tracking_table_template(_config->get_subconfig(WORKLOAD_TRACKING),
            _config->get_bool(COMPRESSION_ENABLED), *_timestamp_manager);
    }

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customize your test. */
        test::run();
    }

    void
    populate(test_harness::database &database, test_harness::timestamp_manager *tsm,
      test_harness::configuration *config, test_harness::workload_tracking *tracking) override final
    {
        std::cout << "populate: nothing done." << std::endl;
    }

    void
    custom_operation(test_harness::thread_context *tc) override final
    {
        std::cout << "custom_operation: nothing done." << std::endl;
    }

    void
    insert_operation(test_harness::thread_context *tc) override final
    {
        std::cout << "insert_operation: nothing done." << std::endl;
    }

    void
    read_operation(test_harness::thread_context *tc) override final
    {
        std::cout << "read_operation: nothing done." << std::endl;
    }

    void
    remove_operation(test_harness::thread_context *tc) override final
    {
        std::cout << "remove_operation: nothing done." << std::endl;
    }

    void
    update_operation(test_harness::thread_context *tc) override final
    {
        std::cout << "update_operation: nothing done." << std::endl;
    }

    void
    validate(bool custom_tracking_table, const std::string &operation_table_name,
      const std::string &schema_table_name,
      const std::vector<uint64_t> &known_collection_ids) override final
    {
        std::cout << "validate: nothing done." << std::endl;
    }
};
