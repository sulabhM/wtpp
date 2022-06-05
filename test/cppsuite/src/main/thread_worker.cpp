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

#include "thread_worker.h"

#include <thread>

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "transaction.h"

namespace test_harness {

const std::string
type_string(thread_type type)
{
    switch (type) {
    case thread_type::CHECKPOINT:
        return ("checkpoint");
    case thread_type::CUSTOM:
        return ("custom");
    case thread_type::INSERT:
        return ("insert");
    case thread_type::READ:
        return ("read");
    case thread_type::REMOVE:
        return ("remove");
    case thread_type::UPDATE:
        return ("update");
    default:
        testutil_die(EINVAL, "unexpected thread_type: %d", static_cast<int>(type));
    }
}

thread_worker::thread_worker(uint64_t id, thread_type type, Configuration *config,
  ScopedSession &&created_session, TimestampManager *timestamp_manager,
  OperationTracker *op_tracker, Database &dbase)
    : /* These won't exist for certain threads which is why we use optional here. */
      collection_count(config->GetOptionalInt(collectionCount, 1)),
      key_count(config->GetOptionalInt(keyCountPerCollection, 1)),
      key_size(config->GetOptionalInt(keySize, 1)),
      value_size(config->GetOptionalInt(valueSize, 1)), thread_count(config->GetInt(threadCount)),
      type(type), id(id), db(dbase), session(std::move(created_session)), tsm(timestamp_manager),
      txn(Transaction(config, timestamp_manager, session.Get())), op_tracker(op_tracker),
      _sleep_time_ms(config->GetThrottleMs())
{
    if (op_tracker->IsEnabled())
        op_track_cursor = session.OpenScopedCursor(op_tracker->getOperationTableName());

    testutil_assert(key_size > 0 && value_size > 0);
}

void
thread_worker::finish()
{
    _running = false;
}

std::string
thread_worker::pad_string(const std::string &value, uint64_t size)
{
    uint64_t diff = size > value.size() ? size - value.size() : 0;
    std::string s(diff, '0');
    return (s.append(value));
}

bool
thread_worker::update(
  ScopedCursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(op_tracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t ts = tsm->GetNextTimestamp();
    ret = txn.SetCommitTimestamp(ts);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        txn.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    cursor->set_value(cursor.Get(), value.c_str());
    ret = cursor->update(cursor.Get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            txn.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to update a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = op_tracker->save_operation(
      txn_id, trackingOperation::INSERT, collection_id, key, value, ts, op_track_cursor);

    if (ret == 0)
        txn.IncrementOp();
    else if (ret == WT_ROLLBACK)
        txn.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save an update to the tracking table");
    return (ret == 0);
}

bool
thread_worker::insert(
  ScopedCursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(op_tracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t ts = tsm->GetNextTimestamp();
    ret = txn.SetCommitTimestamp(ts);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        txn.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    cursor->set_value(cursor.Get(), value.c_str());
    ret = cursor->insert(cursor.Get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            txn.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to insert a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = op_tracker->save_operation(
      txn_id, trackingOperation::INSERT, collection_id, key, value, ts, op_track_cursor);

    if (ret == 0)
        txn.IncrementOp();
    else if (ret == WT_ROLLBACK)
        txn.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save an insert to the tracking table");
    return (ret == 0);
}

bool
thread_worker::remove(ScopedCursor &cursor, uint64_t collection_id, const std::string &key)
{
    WT_DECL_RET;
    testutil_assert(op_tracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t ts = tsm->GetNextTimestamp();
    ret = txn.SetCommitTimestamp(ts);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        txn.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    ret = cursor->remove(cursor.Get());
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            txn.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to remove a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = op_tracker->save_operation(
      txn_id, trackingOperation::DELETE_KEY, collection_id, key, "", ts, op_track_cursor);

    if (ret == 0)
        txn.IncrementOp();
    else if (ret == WT_ROLLBACK)
        txn.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save a remove to the tracking table");
    return (ret == 0);
}

void
thread_worker::sleep()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(_sleep_time_ms));
}

bool
thread_worker::running() const
{
    return (_running);
}
} // namespace test_harness
