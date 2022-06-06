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

#ifndef OPERATION_CONFIGURATION_H
#define OPERATION_CONFIGURATION_H

#include <functional>

#include "configuration.h"
#include "database_operation.h"
#include "thread_worker.h"

namespace test_harness {
/*
 * Helper class to enable scalable operation types in the DatabaseOperation.
 */
class OperationConfiguration {
    public:
    OperationConfiguration(Configuration *config, ThreadType type);

    /* Returns a function pointer to the member function of the supplied database operation. */
    std::function<void(ThreadWorker *)> GetFunction(DatabaseOperation *databaseOperation);

    public:
    Configuration *config;
    const ThreadType type;
    const int64_t threadCount;
};
} // namespace test_harness
#endif
