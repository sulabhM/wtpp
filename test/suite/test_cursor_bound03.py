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

# test_cursor_bound03.py
#    Test that setting bounds of different key formats works in the cursor bound API. Make
# sure that WiredTiger complains when the upper and lower bounds overlap and that clearing the 
# bounds through the bound API and reset calls work appriopately.
class test_cursor_bound03(wttest.WiredTigerTestCase):
    file_name = 'test_cursor_bound03'

    types = [
        ('file', dict(uri='file:',use_colgroup=False)),
        ('table', dict(uri='table:',use_colgroup=False))
    ]

    key_format_values = [
        ('string', dict(key_format='S',value_format='S')),
        ('var', dict(key_format='r',value_format='S')),
        ('fix', dict(key_format='r',value_format='8t')),
        ('int', dict(key_format='i',value_format='S')),
        #('bytes', dict(key_format='u',value_format='S')),
    ]

    config = [
        ('inclusive-evict', dict(inclusive=True,evict=True)),
        #('no-inclusive-evict', dict(inclusive=False,evict=True)),
        ('inclusive', dict(inclusive=True,evict=False)),
        #('no-inclusive', dict(inclusive=False,evict=False))      
    ]

    scenarios = make_scenarios(types, key_format_values, config)
 
    def gen_key(self, i):
        tuple_key = []
        for key in self.key_format:
            if key == 'S' or key == 'u':
                tuple_key.append(str(i))
            elif key == "r":
                tuple_key.append(self.recno(i))
            elif key == "i":
                tuple_key.append(i)
        
        if (len(self.key_format) == 1):
            return tuple_key[0]
        else:
            return tuple(tuple_key)
            
    def gen_value(self, i):
        return 'value' + str(i)

    def set_bounds(self, cursor, bound_config):
        inclusive_config = ",inclusive=false" if self.inclusive == False else ""
        self.assertEqual(cursor.bound("bound={0}{1}".format(bound_config, inclusive_config)), 0)

    def create_session_and_cursor(self):
        uri = self.uri + self.file_name
        create_params = 'value_format=S,key_format={}'.format(self.key_format)
        self.session.create(uri, create_params)

        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(40, 60):
            cursor[self.gen_key(i)] = self.gen_value(i) 
        self.session.commit_transaction()

        if (self.evict):
            evict_cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
            for i in range(40, 60):
                evict_cursor.set_key(self.gen_key(i))
                evict_cursor.search()
                evict_cursor.reset() 

        return cursor

    # Need to also test if we return WT_NOTFOUND.
    def cursor_traversal_bound(self, cursor, lower_key, upper_key, next, expected_count):
        if (upper_key):
            cursor.set_key(self.gen_key(upper_key))
            self.set_bounds(cursor,"upper")
        
        if (lower_key):
            #Set lower bound to test that cursor positioning works.
            cursor.set_key(self.gen_key(lower_key))
            self.set_bounds(cursor,"lower")

        count = ret = 0
        while True:
            if (next):
                ret = cursor.next()
            else:
                ret = cursor.prev()
            self.assertTrue(ret == 0 or ret == wiredtiger.WT_NOTFOUND)
            if ret == wiredtiger.WT_NOTFOUND:
                break
            count += 1
            key = cursor.get_key()
            
            if self.inclusive:
                if (lower_key):
                    self.assertTrue(self.gen_key(lower_key) <= self.gen_key(key))
                if (upper_key):
                    self.assertTrue(key <= self.gen_key(upper_key))
            else:
                if (lower_key):
                    self.assertTrue(self.gen_key(lower_key) < self.gen_key(key))
                if (upper_key):
                    self.assertTrue(self.gen_key(key) < self.gen_key(upper_key))
        self.assertEqual(expected_count, count)
        self.assertEqual(cursor.bound("action=clear"), 0)

    def test_bound_next_early_exit(self):
        cursor = self.create_session_and_cursor()
        self.tty("NEXT TESTS----------------")

        # Upper bound set, default inclusive options works.
        cursor.reset()
        self.cursor_traversal_bound(cursor, None, 50, True, 10)
        cursor.reset()
        self.cursor_traversal_bound(cursor, 45, None, True, 15)
        cursor.reset()
        self.cursor_traversal_bound(cursor, 45, 50, True, 5)
        cursor.reset()
        self.cursor_traversal_bound(cursor, 35, None, True, 20)
            
        # Tests return WT_NOTFOUND
        cursor.reset()
        self.cursor_traversal_bound(cursor, None, 70, True, 20)
        cursor.reset()
        self.cursor_traversal_bound(cursor, 35, 70, True, 20)
        cursor.reset()
        self.cursor_traversal_bound(cursor, 65, 90, True, 0)
        
        cursor.reset()
        self.cursor_traversal_bound(cursor, 45, None, True, 15)

        cursor.reset()
        self.cursor_traversal_bound(cursor, None, 50, True, 10)
        
        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=upper")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.gen_key(45))
        cursor.reset()

        cursor.set_key(self.gen_key(45))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=lower")
        self.assertEqual(cursor.next(), 0)
        key = cursor.get_key()
        self.assertEqual(key, self.gen_key(40))
        cursor.reset()

        cursor.set_key(self.gen_key(50))
        self.set_bounds(cursor,"lower")
        cursor.bound("action=clear,bound=upper")
        self.cursor_traversal_bound(cursor, None, None, True, 10)

        cursor.bound("action=clear,bound=lower")
        self.cursor_traversal_bound(cursor, None, None, True, 20)

        # Test case where cursor is positioned at valid position and we call bounds on something totally irrelevant
        # Test case 1: Cursor traverses to lower bound 
        # Test case 2: Cursor early exists with upper bound
        # Test case 3: 1 + 2
        # Test case 4: Cursor traverses to lower bound (out of data range)
        # Test case 5: Cursor traverses to upper bound (out of data range)
        # Test case 6: 4 + 5
        # Test case: 4 + 5 (out of data range)
        # Test case 7: Test that cursor reset, clears the bounds
        # Test case 8: Test that cursor action clear, clears the bounds
        # Test case: Test that cursor traversal restarts at bounds
        # Complex cases
        # Test case 9: Test that cursor traverses to lower bound (in) and then change lower bound (in) again
        # Test case 10: Test that cursor traverses to lower bound (in) and then change lower bound (out) again
        # Test case 11: Test that cursor traverses to lower bound (out) and then change lower bound (in) again
        # Test case 12: Test that cursor traverses to lower bound (in) and then change lower bound (out) again
        # Test case 13: Test that cursor traverses to upper bound (in) and then change upper bound (in) again
        # Test case 14: Test that cursor traverses to upper bound (in) and then change upper bound (out) again
        # Test case 15: Test that cursor traverses to upper bound (out) and then change upper bound (in) again
        # Test case 16: Test that cursor traverses to upper bound (in) and then change upper bound (out) again
        # Test case 17: Test that cursor traverses to lower bound (in) with upper bound (in) and then change lower bound (out) again
        # Test case 18: Test that cursor traverses to lower bound (out) with upper bound (in) and then change lower bound (in) again
        # Test case 19: Test that cursor traverses to lower bound (in) with upper bound (in) and then change lower bound (in) again
        # Test case 20: Test that cursor traverses to lower bound (out) with upper bound (in) and then change lower bound (out) again
        # Test case 21: Test that cursor traverses to upper bound (in) with lower bound (in) and then change upper bound (out) again
        # Test case 22: Test that cursor traverses to upper bound (out) with lower bound (in) and then change upper bound (in) again
        # Test case 23: Test that cursor traverses to upper bound (in) with lower bound (in) and then change upper bound (in) again
        # Test case 24: Test that cursor traverses to upper bound (out) with lower bound (in) and then change upper bound (out) again
        # Test case 25: Test that cursor traverses to lower bound (in) with upper bound (out) and then change lower bound (out) again
        # Test case 26: Test that cursor traverses to lower bound (out) with upper bound (out) and then change lower bound (in) again
        # Test case 27: Test that cursor traverses to lower bound (in) with upper bound (out) and then change lower bound (in) again
        # Test case 28: Test that cursor traverses to lower bound (out) with upper bound (out) and then change lower bound (out) again
        # Test case 29: Test that cursor traverses to upper bound (in) with lower bound (out) and then change upper bound (out) again
        # Test case 30: Test that cursor traverses to upper bound (out) with lower bound (out) and then change upper bound (in) again
        # Test case 31: Test that cursor traverses to upper bound (in) with lower bound (out) and then change upper bound (in) again
        # Test case 32: Test that cursor traverses to upper bound (out) with lower bound (out) and then change upper bound (out) again
        # Test case 32 -> 56: Test that cursor traverses works (mid-way)
        # Test case 57: Test that cursor resets works mid-way through traversal
        # Test case 57 -> 81: Test that cursor action clear works mid-way through traversal
        # Test inclusive cases:
        # Test case 58: Cursor traverses to lower bound 
        # Test case 59: Cursor early exists with upper bound
        # Test case 60: 1 + 2
        # Test case 61: Cursor traverses to lower bound (out of data range)
        # Test case 62: Cursor traverses to upper bound (out of data range)
        # Test case 63: 4 + 5
        # Test case 64: Test that cursor reset, clears the bounds AND inclusive
        # Test case 65: Test that cursor action clear, clears the bounds AND inclusive
        # Test complex inclusive cases:
        # Test case 65 -> 89: Test that cursor traversal works with change to inclusive mid-way works too.
        # Test combination cases:
        # Test that next() and prev() works while traversing
        # Test that next() EOF, then prev() with bounds works
        # Test that prev() EOF, then next() with bounds works

        cursor.close()

    def bound_prev_early_exit(self):
        cursor = self.create_session_and_cursor()

        # Lower bound set, default inclusive options works.
        #self.cursor_traversal_bound(cursor, None, 50, False)
        #self.cursor_traversal_bound(cursor, 45, 50, False)
        self.cursor_traversal_bound(cursor, 35, None, False)
        self.cursor_traversal_bound(cursor, 50, None, False)

        # cursor.set_key(self.gen_key(70))
        # self.set_bounds(cursor,"upper")
        # self.assertEqual(cursor.prev(), wiredtiger.WT_NOTFOUND)

        cursor.close()

if __name__ == '__main__':
    wttest.run()