#!/usr/bin/env python
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import wiredtiger, wttest
from wtscenario import make_scenarios
from wtbound import bound_base

# test_prepare22.py
#    Prepare: Cursor API usage generates expected error in prepared state with bounded cursors.

# Pattern of test script is to invoke cursor operations in prepared transaction
# state to ensure they fail and to repeat same operations in non-prepared state
# to ensure normally they pass.
class test_prepare22(bound_base):
    """
    Test basic operations
    """
    file_name = 'test_prepare_cursor22'
    nentries = 10

    types = [
        ('file', dict(uri='file:', use_colgroup=False)),
        ('table', dict(uri='table:', use_colgroup=False)),
        ('colgroup', dict(uri='table:', use_colgroup=True))
    ]

    key_format_values = [
        ('string', dict(key_format='S')),
        # FIXME-WT-9474: Uncomment once column store is implemented.
        # ('var', dict(key_format='r')),
        ('int', dict(key_format='i')),
        ('bytes', dict(key_format='u')),
        ('composite_string', dict(key_format='SSS')),
        ('composite_int_string', dict(key_format='iS')),
        ('composite_complex', dict(key_format='iSru')),
    ]

    scenarios = make_scenarios(types, key_format_values)

    def assertCursorHasNoKeyValue(self, cursor):
        keymsg = '/requires key be set/'
        valuemsg = '/requires value be set/'
        self.assertRaisesWithMessage(
            wiredtiger.WiredTigerError, cursor.get_key, keymsg)
        self.assertRaisesWithMessage(
            wiredtiger.WiredTigerError, cursor.get_value, valuemsg)

    def create_session_and_cursor(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format={}'.format(self.key_format)
        if self.use_colgroup:
            create_params += self.gen_colgroup_create_param()
        self.session.create(uri, create_params)
        # Add in column group.
        if self.use_colgroup:
            create_params = 'columns=(v),'
            suburi = 'colgroup:{0}:g0'.format(self.file_name)
            self.session.create(suburi, create_params)

        cursor = self.session.open_cursor(uri)

        self.session.begin_transaction()
        for i in range(self.start_key, self.end_key + 1):
            cursor[self.gen_key(i)] = "value" + str(i)
        self.session.commit_transaction()

        return cursor

    # Create the table and test basic prepare cursor operations.
    def test_basic_prepared(self):
        preparemsg = "/ not permitted in a prepared transaction/"

        cursor = self.create_session_and_cursor()
        self.set_bounds(cursor, 45, "lower", inclusive = True)
        
        # Check search/search_near operations
        # If the cursor key is outside the bounds, check that a search on a prepared transaction fails.
        self.session.begin_transaction()
        cursor.set_key(self.gen_key(50))
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(3))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor.search(), preparemsg)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(3))
        self.session.timestamp_transaction('durable_timestamp=' + self.timestamp_str(3))
        self.session.commit_transaction()
        
        self.session.begin_transaction()
        cursor.set_key(self.gen_key(50))
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(3))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor.search_near(), preparemsg)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(3))
        self.session.timestamp_transaction('durable_timestamp=' + self.timestamp_str(3))
        self.session.commit_transaction()

        # Check next/prev operations
        self.session.begin_transaction()
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(3))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor.next(), preparemsg)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(3))
        self.session.timestamp_transaction('durable_timestamp=' + self.timestamp_str(3))
        self.session.commit_transaction()
        
        self.session.begin_transaction()
        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(3))
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor.prev(), preparemsg)
        self.session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(3))
        self.session.timestamp_transaction('durable_timestamp=' + self.timestamp_str(3))
        self.session.commit_transaction()

    
    # Test ignore prepare.
    def test_ignore_prepared(self):
        uri = 'table:test_ignore_prepare_scenario'
        preparemsg = "/ not permitted in a prepared transaction/"

        self.session.create(uri, 'key_format=u,value_format=u')
        cursor = self.session.open_cursor(uri)

        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(uri)
        l = "abcdefghijklmnopqrstuvwxyz"
        self.session.begin_transaction()
        
        session2.begin_transaction()
        # Prepare keys aa -> zz
        for i in range (0, 26):
            if (i == 2):
                continue
            for j in range (0, 26):
                cursor2[l[i] + l[j]] = l[i] + l[j]

        session2.prepare_transaction('prepare_timestamp=2')

        # Evict the whole range.
        for i in range (0, 26):
            for j in range(0, 26):
                cursor.set_key(l[i] + l[j])
                cursor.search()
                cursor.reset()

        self.set_bounds(cursor, 'aa', "lower", inclusive = True)
        self.set_bounds(cursor, 'zz', "upper", inclusive = True)
        self.session.commit_transaction()
        self.session.begin_transaction("ignore_prepare=true")
        self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor.next(), wiredtiger.WT_NOTFOUND)
        self.session.commit_transaction()
        
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor2.prev(), preparemsg)
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda:cursor2.next(), preparemsg)
        session2.rollback_transaction()
        self.assertEqual(cursor2.prev(), wiredtiger.WT_NOTFOUND)
        self.assertEqual(cursor2.next(), wiredtiger.WT_NOTFOUND)
            


