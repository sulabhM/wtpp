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

namespace test_harness {

/*
 * Class that defines operations that do nothing as an example. This shows how database operations
 * can be overridden and customized.
 */
class test_bug010 : public test {
    public:
    test_bug010(const test_args &args) : test(args)
    {
    }

    void
    run() override final
    {
        /* You can remove the call to the base class to fully customize your test. */
        test::run();
    }

    void
    checkpoint_operation(thread_worker *) override final
    {
        logger::log_msg(LOG_WARN, "checkpoint_operation: nothing done");
    }

    void
    update_operation(thread_worker *tw) override final
    {
        logger::log_msg(LOG_WARN, "update_operation: nothing done");

        char *validate_value;

        // Retrieve the collection created by the populate operation.
        collection &coll = tw->db.get_collection(0);
        scoped_cursor cursor = tw->session.open_scoped_cursor(coll.name);


        while (tw->running()) {

            //bool success = true;
            // Each transaction should have target_op_count insert operations. This value is set in the configuration file
            for(int i = 0; i < 100; ++i) {
                
                tw->sleep();

                tw->txn.begin();
        
                // Populate the key and value with random strings. We only use the size of the value in this test
                const std::string key = "a";
                const std::string value = std::to_string(i);

                if(!tw->update(cursor, coll.id, key, value)) {
                    //success = false;
                    testutil_die(1, "Exit Update");
                }
                else {
                    testutil_assert(tw->txn.commit());
                }

                // Validation
                scoped_cursor validate_cursor = tw->session.open_scoped_cursor(coll.name);
                validate_cursor->set_key(validate_cursor.get(),"a");
                validate_cursor->get_value(validate_cursor.get(), "a", &validate_value);

                int ii = std::stoi(validate_value);
                testutil_assert(i == ii);
            }


        }
    }

    }; // namespace test_harness
}
