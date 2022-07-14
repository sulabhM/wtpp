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
#include "src/common/logger.h"
#include "src/main/test.h"

#include <iostream>

namespace test_harness {
/* Defines what data is written to the tracking table for use in custom validation. */
class operation_tracker_test_bug010 : public operation_tracker {

    public:
    operation_tracker_test_bug010(
      configuration *config, const bool use_compression, timestamp_manager &tsm)
        : operation_tracker(config, use_compression, tsm)
    {
    }

    void
    set_tracking_cursor(WT_SESSION *session, const tracking_operation &operation,
      const uint64_t &collection_id, const std::string &key, const std::string &value,
      wt_timestamp_t ts, scoped_cursor &op_track_cursor) override final
    {
        /* You can replace this call to define your own tracking table contents. */
        operation_tracker::set_tracking_cursor(
          session, operation, collection_id, key, value, ts, op_track_cursor);
    }
};

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class test_bug010 : public test {
    public:
    test_bug010(const test_args &args) : test(args)
    {
        init_operation_tracker(
          new operation_tracker_test_bug010(_config->get_subconfig(OPERATION_TRACKER),
            _config->get_bool(COMPRESSION_ENABLED), *_timestamp_manager));
    }

    void
    update_operation(thread_worker *tc) override final
    {
        /* Retrieve the number of collections created during the populate phase. */
        auto collection_count = tc->db.get_collection_count();
        std::cout << "collection count is " << collection_count << std::endl;
        
        /* Open a cursor on each collection and save them. */
        std::map<uint64_t, scoped_cursor> cursors;
        for (uint64_t i = 0; i < collection_count; ++i) {
            collection &coll = tc->db.get_collection(i);
            logger::log_msg(LOG_TRACE,
              "Thread {" + std::to_string(tc->id) +
                "} Creating cursor for collection: " + coll.name);
            scoped_cursor cursor = tc->session.open_scoped_cursor(coll.name);
            cursors.emplace(coll.id, std::move(cursor));
        }

        uint64_t iteration = 0;

        while (tc->running()) {
            /* Generate the next value for each record using the current iteration. */
            auto value = tc->pad_string(std::to_string(iteration), tc->key_size);
            std::cout << "Value is " << value << std::endl;

            /* Go through each collection to update. */
            for (uint64_t i = 0; i < collection_count; ++i) {

                /* Retrieve the collection info and the cursor associated with it. */
                collection &coll = tc->db.get_collection(i);
                scoped_cursor &cursor = cursors[coll.id];

                tc->txn.begin();
                /* 
                 * Each collection has one record, calling next should lead us to the first existing
                 * record.
                 */
                testutil_check(cursor->next(cursor.get()));

                /* Retrieve the key. */
                const char *key_str;
                testutil_check(cursor->get_key(cursor.get(), &key_str));
                std::cout << "Retrieved key is " << key_str << std::endl;

                /* Update the key with the new value. */
                if (tc->update(cursor, coll.id, key_str, value)) {
                    testutil_assert(tc->txn.commit());
                    std::cout << "commit!" << std::endl;
                } else {
                    tc->txn.rollback();
                }
                
                testutil_check(cursor->reset(cursor.get()));
            }

            /* We have processed all the collections, verify what we have done. */
            for (uint64_t i = 0; i < collection_count; ++i) {
                collection &coll = tc->db.get_collection(i);
                scoped_cursor &cursor = cursors[coll.id];

                testutil_check(cursor->next(cursor.get()));

                const char *value_str;
                testutil_check(cursor->get_value(cursor.get(), &value_str));
                testutil_assert(value_str == value);

                testutil_check(cursor->reset(cursor.get()));
            }
        
            /* We have processed all the collections, take some rest and repeat. */
            ++iteration;
            tc->sleep();
        }
    }
};

} // namespace test_harness
