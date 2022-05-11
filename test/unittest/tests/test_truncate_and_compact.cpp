/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*	All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#include <catch2/catch.hpp>
#include <iostream>
#include <vector>
#include <unordered_map>
#include "wt_internal.h"
#include "wiredtiger.h"
#include "extern.h"
#include "utils.h"
#include "wrappers/connection_wrapper.h"

static const std::string testcase_key_base = "key ";
static const std::string testcase_value_base = "a really long string and a value ";

static const std::string testcase_key1 = "key1";
static const std::string testcase_value1 = "value1";

TEST_CASE("Truncate and compact: create simple table", "[compact]")
{
    ConnectionWrapper conn(utils::UnitTestDatabaseHome);
    WT_SESSION_IMPL* sessionImpl = conn.createSession();
    WT_SESSION* session = &(sessionImpl->iface);

    REQUIRE(session->create(session, "table:access", "key_format=S,value_format=S") == 0);

    WT_CURSOR* cursor = nullptr;
    REQUIRE(session->open_cursor(session, "table:access", nullptr, nullptr, &cursor) == 0);

    cursor->set_key(cursor, testcase_key1.c_str());
    cursor->set_value(cursor, testcase_value1.c_str());
    REQUIRE(cursor->insert(cursor) == 0);

    char const* key = nullptr;
    char const* value = nullptr;
    REQUIRE(cursor->reset(cursor) == 0);
    int ret = cursor->next(cursor);
    REQUIRE(ret == 0);
    while (ret == 0) {
        REQUIRE(cursor->get_key(cursor, &key) == 0);
        REQUIRE(cursor->get_value(cursor, &value) == 0);
        REQUIRE(key == testcase_key1);
        REQUIRE(value == testcase_value1);
        ret = cursor->next(cursor);
    }
    REQUIRE(ret == WT_NOTFOUND); /* Check for end-of-table. */
}

int64_t
get_stat(WT_CURSOR *cursor, int stat_field)
{
    cursor->set_key(cursor, stat_field);
    REQUIRE(cursor->search(cursor) == 0);

    const char* desc = nullptr;
    const char* pvalue = nullptr;
    int64_t value = 0;
    REQUIRE(cursor->get_value(cursor, &desc, &pvalue, &value) == 0);
    return value;
}

void dump_stats(WT_SESSION_IMPL * sessionImpl) {
    WT_SESSION* session = &(sessionImpl->iface);
    WT_CURSOR* cursor = nullptr;
    REQUIRE(session->open_cursor(session, "statistics:table:access2", nullptr, nullptr, &cursor) == 0);

    std::cout << "Statistic WT_STAT_DSRC_BTREE_ROW_INTERNAL: "      << get_stat(cursor, WT_STAT_DSRC_BTREE_ROW_INTERNAL)  << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_BTREE_ROW_LEAF: "          << get_stat(cursor, WT_STAT_DSRC_BTREE_ROW_LEAF)      << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_BTREE_MAXIMUM_DEPTH: "     << get_stat(cursor, WT_STAT_DSRC_BTREE_MAXIMUM_DEPTH) << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_CACHE_STATE_PAGES_CLEAN: " << get_stat(cursor, WT_STAT_DSRC_CACHE_STATE_PAGES_CLEAN) << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_CACHE_STATE_PAGES_DIRTY: " << get_stat(cursor, WT_STAT_DSRC_CACHE_STATE_PAGES_DIRTY) << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_CACHE_READ_DELETED: "      << get_stat(cursor, WT_STAT_DSRC_CACHE_READ_DELETED) << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_REC_PAGE_DELETE_FAST: "    << get_stat(cursor, WT_STAT_DSRC_REC_PAGE_DELETE_FAST) << std::endl;
    std::cout << "Statistic WT_STAT_DSRC_REC_PAGE_DELETE: "         << get_stat(cursor, WT_STAT_DSRC_REC_PAGE_DELETE) << std::endl;
    int64_t total = get_stat(cursor, WT_STAT_DSRC_BTREE_ROW_INTERNAL) + get_stat(cursor, WT_STAT_DSRC_BTREE_ROW_LEAF) ;
    std::cout << "Internal + leaf: " << total << std::endl;
}

uint64_t get_num_key_values(WT_SESSION* session, std::string const& table_name, uint64_t timeStamp)
{
    WT_CURSOR* cursor = nullptr;
    REQUIRE(session->open_cursor(session, table_name.c_str(), nullptr, nullptr, &cursor) == 0);

    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    REQUIRE(session->timestamp_transaction_uint(session, WT_TS_TXN_TYPE_READ, timeStamp) == 0);
    char const *key = nullptr;
    char const *value = nullptr;
    REQUIRE(cursor->reset(cursor) == 0);
    int ret = cursor->next(cursor);
    REQUIRE(ret == 0);
    uint64_t numValues = 0;
    while (ret == 0) {
        REQUIRE(cursor->get_key(cursor, &key) == 0);
        REQUIRE(cursor->get_value(cursor, &value) == 0);
        numValues++;
        //            std::cout << key << " : " << value <<
        //              " (via struct, key : " << (const char*) cursor->key.data <<
        //              ", value : " << (const char*) cursor->value.data << ")" << std::endl;
        ret = cursor->next(cursor);
    }
    REQUIRE(ret == WT_NOTFOUND); // Check for end-of-table.
    REQUIRE(session->commit_transaction(session, nullptr) == 0);
    std::cout << "number of key:value pairs: " << numValues <<
      " at timestamp: 0x" << std::hex << timeStamp << std::dec << std::endl;
    REQUIRE(cursor->close(cursor) == 0);
    return numValues;
}


int depth_in_tree(WT_REF* ref) {
    int depth = 0;

    while(ref->home != nullptr) {
        ++depth;
        WT_PAGE* home = ref->home;
        ref = home->u.intl.parent_ref;
    }
    return depth;
}


void dump_ref_map(std::unordered_multimap<WT_REF*, WT_REF*> const& ref_map, WT_REF* parent)
{
    REQUIRE(parent != nullptr);
    int depth = depth_in_tree(parent);
    std::string indent(2 * depth, ' ');
    auto range = ref_map.equal_range(parent);
    for (auto iter = range.first; iter != range.second; ++iter) {
        if (F_ISSET(iter->first, WT_REF_FLAG_INTERNAL)) {
            std::cout << indent << "depth: " << depth << ": parent ref = " << iter->first
                      << ", child ref =" << iter->second << std::endl;
            dump_ref_map(ref_map, iter->second);
        }
    }
}


void
cache_walk(WT_SESSION_IMPL *session)
{
    std::cout << "cache_walk:" << std::endl;
    std::unordered_multimap<WT_REF*, WT_REF*> ref_map; // Maps parents to children
    WT_BTREE *btree;
    WT_CACHE *cache;
    WT_PAGE *page;
    WT_REF *next_walk;
    uint64_t dsk_size, gen_gap, gen_gap_max, gen_gap_sum, max_pagesize;
    uint64_t min_written_size, num_memory, num_not_queueable, num_queued;
    uint64_t num_smaller_allocsz, pages_clean, pages_dirty, pages_internal;
    uint64_t pages_leaf, seen_count, visited_count;
    uint64_t visited_age_gap_sum, unvisited_count, unvisited_age_gap_sum;
    uint64_t walk_count, written_size_cnt, written_size_sum;

    btree = S2BT(session);
    cache = S2C(session)->cache;
    gen_gap_max = gen_gap_sum = max_pagesize = 0;
    num_memory = num_not_queueable = num_queued = 0;
    num_smaller_allocsz = pages_clean = pages_dirty = pages_internal = 0;
    pages_leaf = seen_count = visited_count = 0;
    visited_age_gap_sum = unvisited_count = unvisited_age_gap_sum = 0;
    walk_count = written_size_cnt = written_size_sum = 0;
    min_written_size = UINT64_MAX;

    std::vector<int> ref_state_counts(std::numeric_limits<uint8_t>::max(), 0);

    WT_REF* root = nullptr;
    next_walk = NULL;
    while (__wt_tree_walk_count(session, &next_walk, &walk_count,
             WT_READ_CACHE | WT_READ_NO_EVICT | WT_READ_NO_GEN | WT_READ_NO_WAIT |
               WT_READ_VISIBLE_ALL) == 0 &&
      next_walk != NULL) {
        ++seen_count;
        page = next_walk->page;
        ++ref_state_counts[next_walk->state];
        if (page->memory_footprint > max_pagesize)
            max_pagesize = page->memory_footprint;

        if (__wt_page_is_modified(page))
            ++pages_dirty;
        else
            ++pages_clean;

        if (!__wt_ref_is_root(next_walk) && !__wt_page_can_evict(session, next_walk, NULL))
            ++num_not_queueable;

        if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU))
            ++num_queued;

        dsk_size = page->dsk != NULL ? page->dsk->mem_size : 0;
        if (dsk_size != 0) {
            if (dsk_size < btree->allocsize)
                ++num_smaller_allocsz;
            if (dsk_size < min_written_size)
                min_written_size = dsk_size;
            ++written_size_cnt;
            written_size_sum += dsk_size;
        } else
            ++num_memory;

        int depth = depth_in_tree(next_walk);
        std::string indent(2 * depth, ' ');

        if (F_ISSET(next_walk, WT_REF_FLAG_INTERNAL)) {
            ++pages_internal;
            //std::cout << indent << "Internal page: " << page << std::endl;
        } else {
            ++pages_leaf;
//            std::cout << indent << "Leaf page: " << page << std::endl;
        }

        if (next_walk->home != nullptr) {
            WT_REF *parent_ref = next_walk->home->u.intl.parent_ref;
            //std::cout << indent << "  ref: " << next_walk << ", parent ref: " << parent_ref << std::endl;
            ref_map.insert({parent_ref, next_walk});
        }

        // Debugging only
//        if (F_ISSET(next_walk, WT_REF_FLAG_INTERNAL))
//        {
//            std::cout << indent << "  ref: " << next_walk << std::endl;
//            std::cout << indent << "  home: " << next_walk->home << std::endl;
//            if (next_walk->home != nullptr) {
//                WT_REF *parent_ref = next_walk->home->u.intl.parent_ref;
//                std::cout << indent << "  parent ref: " << parent_ref << std::endl;
//            }
//            std::cout << indent << "  depth in tree: " << depth << std::endl;
//
//            {
//                uint8_t previous_state = 0;
//                WT_REF_LOCK(session, next_walk, &previous_state);
//                std::cout << indent << "  state: " << (int)previous_state << std::endl;
//                WT_REF_UNLOCK(next_walk, previous_state);
//            }
//        }

        /* Skip root pages since they are never considered */
        if (__wt_ref_is_root(next_walk)) {
            root = next_walk;
            continue;
        }

        if (page->evict_pass_gen == 0) {
            unvisited_age_gap_sum += (cache->evict_pass_gen - page->cache_create_gen);
            ++unvisited_count;
        } else {
            visited_age_gap_sum += (cache->evict_pass_gen - page->cache_create_gen);
            gen_gap = cache->evict_pass_gen - page->evict_pass_gen;
            if (gen_gap > gen_gap_max)
                gen_gap_max = gen_gap;
            gen_gap_sum += gen_gap;
            ++visited_count;
        }
    }
    std::cout << "ending cache walk, root = " << root << std::endl;
    std::cout << "WT_REF count with state WT_REF_DISK:    " << ref_state_counts[WT_REF_DISK] << std::endl;
    std::cout << "WT_REF count with state WT_REF_DELETED: " << ref_state_counts[WT_REF_DELETED] << std::endl;
    std::cout << "WT_REF count with state WT_REF_LOCKED:  " << ref_state_counts[WT_REF_LOCKED] << std::endl;
    std::cout << "WT_REF count with state WT_REF_MEM:     " << ref_state_counts[WT_REF_MEM] << std::endl;
    std::cout << "WT_REF count with state WT_REF_SPLIT:   " << ref_state_counts[WT_REF_SPLIT] << std::endl;
    //dump_ref_map(ref_map, root);
}

void analyse_tree(WT_SESSION_IMPL* sessionImpl, std::string const& file_name) {
    std::cout << "Analysing the tree" << std::endl;
    // Analyse the btree
    REQUIRE(__wt_session_get_dhandle(sessionImpl, file_name.c_str(), nullptr, nullptr, 0) == 0);
    REQUIRE(sessionImpl->dhandle != nullptr);
    WT_BTREE* btree = S2BT(sessionImpl);
    REQUIRE(btree != nullptr);
    WT_REF* ref = &btree->root;
    REQUIRE(ref != nullptr);
    //REQUIRE(__wt_debug_tree_all(sessionImpl, nullptr, ref, nullptr) == 0);
    __wt_curstat_cache_walk(sessionImpl);
    //dump_stats(sessionImpl);
    cache_walk(sessionImpl);
}


TEST_CASE("Truncate and compact: table", "[compact]")
{
    // The goal of this test is to ensure that truncate and compact work together
    //
    // The steps in this test are:
    // 1. Add a large number of key/values to a database with small pages,
    //    so that many subtrees are created.
    // 2. Truncate part of the tree, so that at least one subtree is deleted.
    // 3. Perform a cursor traversal on the tree, at a time prior to the truncate
    // 4. Run a compact operation, while a reader is trying to
    //    read some of the data deleted by the truncate, and ensure that this works.

    ConnectionWrapper conn(utils::UnitTestDatabaseHome);
    WT_SESSION_IMPL* sessionImpl = conn.createSession();
    WT_SESSION* session = &(sessionImpl->iface);
    std::string table_name = "table:access2";
    std::string file_name = "file:access2.wt";

    // maybe these are too small
    std::string config =
      "key_format=S,value_format=S,allocation_size=1024b,internal_page_max=1024b,leaf_page_max=1024b";
    REQUIRE(session->create(session, table_name.c_str(), config.c_str()) == 0);

    //    set oldest and stable timestamps
    std::cout << "Set oldest and stable timestamps to 0x1" << std::endl;
    REQUIRE(conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "oldest_timestamp=1") == 0);
    REQUIRE(conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "stable_timestamp=1") == 0);

    dump_stats(sessionImpl);

    {
        WT_CURSOR* cursor = nullptr;
        REQUIRE(session->open_cursor(session, table_name.c_str(), nullptr, nullptr, &cursor) == 0);

        // Add some key/value pairs, with timestamp 0x10
        std::cout << "Add some key/value pairs" << std::endl;
        int maxOuter = 100;
        int maxInner = 1000;
        for (int outer = 0; outer < maxOuter; outer++) {
            REQUIRE(session->begin_transaction(session, nullptr) == 0);
            for (int inner = 0; inner < maxInner; inner++) {
                int index = 1000000 + (outer * maxInner) + inner;
                std::basic_string key = testcase_key_base + std::to_string(index);
                std::basic_string value = testcase_value_base + std::to_string(index);
                cursor->set_key(cursor, key.c_str());
                cursor->set_value(cursor, value.c_str());
                REQUIRE(cursor->insert(cursor) == 0);
            }
            std::string transactionConfig = std::string("commit_timestamp=10");
            REQUIRE(session->commit_transaction(session, transactionConfig.data()) == 0); // set ts here.
        }

        REQUIRE(cursor->close(cursor) == 0);
        dump_stats(sessionImpl);
    }

    {
        // Truncate, with timestamp = 0x30
        // Need to trigger fast truncate, which will truncate whole pages at one.
        // Need to fast truncate an internal page as well for this test.
        std::cout << "Truncate" << std::endl;
        REQUIRE(session->begin_transaction(session, nullptr) == 0);

        WT_CURSOR* truncate_start = nullptr;
        REQUIRE(session->open_cursor(session, table_name.c_str(), nullptr, nullptr, &truncate_start) == 0);
        std::string key_start = testcase_key_base + std::to_string(1010000);
        truncate_start->set_key(truncate_start, key_start.c_str());
        REQUIRE(truncate_start->search(truncate_start) == 0);

        WT_CURSOR* truncate_end = nullptr;
        REQUIRE(session->open_cursor(session, table_name.c_str(), nullptr, nullptr, &truncate_end) == 0);
        std::string key_end = testcase_key_base + std::to_string(1089999);
        truncate_end->set_key(truncate_end, key_end.c_str());
        REQUIRE(truncate_end->search(truncate_end) == 0);
        REQUIRE(session->truncate(session, nullptr, truncate_start, truncate_end, nullptr) == 0);

        REQUIRE(truncate_start->close(truncate_start) == 0);
        REQUIRE(truncate_end->close(truncate_end) == 0);
        std::string transactionConfig = std::string("commit_timestamp=30");
        REQUIRE(session->commit_transaction(session, transactionConfig.data()) == 0); // set ts here.
        dump_stats(sessionImpl);
        //sleep(5);
    }

    {
        // Read the key/value pairs, at timestamp 0x40 (ie after everything)
        REQUIRE(get_num_key_values(session, table_name, 0x40) == 20000);
    }

    {
        // Compact
        std::cout << "Compact (0):" << std::endl;
        REQUIRE(session->compact(session, table_name.c_str(), nullptr) == 0);
        dump_stats(sessionImpl);
        //sleep(5);
    }

#ifdef HAVE_DIAGNOSTIC
    //analyse_tree(sessionImpl, file_name);
#endif

    {
        std::cout << "Checkpoint (1):" << std::endl;
        REQUIRE(session->checkpoint(session, nullptr) == 0);
        dump_stats(sessionImpl);
        // Compact
        std::cout << "Compact (1):" << std::endl;
        REQUIRE(session->compact(session, table_name.c_str(), nullptr) == 0);
        dump_stats(sessionImpl);
        //sleep(5);
    }
    {
        // Read the key/value pairs, at timestamp 0x20 (ie before the truncate)
        REQUIRE(get_num_key_values(session, table_name, 0x20) == 100000);
    }

    //    set oldest and stable timestamps
    std::cout << "Set oldest and stable timestamps to 0x35" << std::endl;
    REQUIRE(conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "stable_timestamp=35") == 0);
    REQUIRE(conn.getWtConnection()->set_timestamp(conn.getWtConnection(), "oldest_timestamp=35") == 0);
    dump_stats(sessionImpl);

#ifdef HAVE_DIAGNOSTIC
    //analyse_tree(sessionImpl, file_name);
#endif

    {
        // Compact
        std::cout << "Compact (2):" << std::endl;
        REQUIRE(session->compact(session, table_name.c_str(), nullptr) == 0);
        dump_stats(sessionImpl);
        std::cout << "Checkpoint (2):" << std::endl;
        REQUIRE(session->checkpoint(session, nullptr) == 0);
        dump_stats(sessionImpl);
    }

#ifdef HAVE_DIAGNOSTIC
    analyse_tree(sessionImpl, file_name);
#endif

    // Read the key/value pairs, at timestamp 0x40 (ie after everything)
    REQUIRE(get_num_key_values(session, table_name, 0x40) == 20000);

    // TODO: We get a "scratch buffer allocated and never discarded" warning.
    //       It seems to come from __wt_debug_tree_all.
}
