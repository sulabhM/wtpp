/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_array_walk --
 *     Walk the connections session array, calling a function for every active session in the array.
 *     Callers can exit the walk early if desired. Arguments to the walk function are provided by a
 *     customizable cookie.
 *
 * The walk itself cannot fail, if the callback function can't error out then the call to this
 *     function should be wrapped in an ignore return macro.
 */
int
__wt_session_array_walk(WT_SESSION_IMPL *session,
  int (*walk_func)(WT_SESSION_IMPL *, WT_SESSION_IMPL *, bool *exit_walkp, void *cookiep),
  bool skip_internal, void *cookiep)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *array_session;
    uint32_t i, session_cnt;
    u_int active;
    bool exit_walk;

    exit_walk = false;
    conn = S2C(session);

    /*
     * Ensure we read the session count only once. We want to iterate over all sessions that were
     * active at this point in time. Sessions in the array may open, close, or be have their
     * contents change during traversal. We expect the calling code to handle this. See the Slotted
     * array usage pattern in the architecture guide for more details.
     */
    WT_READ_ONCE(session_cnt, conn->session_array.cnt);

    for (i = 0, array_session = WT_CONN_SESSIONS_GET(conn); i < session_cnt; i++, array_session++) {
        /*
         * This acquire read is paired with a WT_RELEASE_WRITE_WITH_BARRIER from the session create
         * logic, and guarantees that by the time this thread sees active == 1 all other fields in
         * the session have been initialized properly. Any other ordering constraints, such as
         * ensuring this loop occurs in-order, are not intentional.
         */
        WT_ACQUIRE_READ_WITH_BARRIER(active, array_session->active);

        /* Skip inactive sessions. */
        if (!active)
            continue;

        /* If configured skip internal sessions. */
        if (skip_internal && F_ISSET(array_session, WT_SESSION_INTERNAL))
            continue;

        WT_RET(walk_func(session, array_session, &exit_walk, cookiep));
        /* Early exit the walk if possible. */
        if (exit_walk)
            break;
    }
    return (0);
}

/*
 * __wt_session_dump --
 *     Given a session dump information about that session. The caller session's scratch memory and
 *     event handler is used.
 */
int
__wt_session_dump(WT_SESSION_IMPL *session, WT_SESSION_IMPL *dump_session, bool show_cursors)
{
    WT_CURSOR *cursor;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;

    WT_ERR(__wt_scr_alloc(session, 0, &buf));

    WT_ERR(__wt_msg(
      session, "Session: ID: %" PRIu32 " @: 0x%p", dump_session->id, (void *)dump_session));
    WT_ERR(
      __wt_msg(session, "  Name: %s", dump_session->name == NULL ? "EMPTY" : dump_session->name));
    if (!show_cursors) {
        WT_ERR(__wt_msg(session, "  Last operation: %s",
          dump_session->lastop == NULL ? "NONE" : dump_session->lastop));
        WT_ERR(__wt_msg(session, "  Current dhandle: %s",
          dump_session->dhandle == NULL ? "NONE" : dump_session->dhandle->name));
        WT_ERR(__wt_msg(
          session, "  Backup in progress: %s", dump_session->bkp_cursor == NULL ? "no" : "yes"));
        WT_ERR(__wt_msg(session, "  Compact state: %s",
          dump_session->compact_state == WT_COMPACT_NONE ?
            "none" :
            (dump_session->compact_state == WT_COMPACT_RUNNING ? "running" : "success")));
        WT_ERR(__wt_msg(session, "  Flags: 0x%" PRIx32, dump_session->flags));
        WT_ERR(__wt_msg(session, "  Isolation level: %s",
          dump_session->isolation == WT_ISO_READ_COMMITTED ?
            "read-committed" :
            (dump_session->isolation == WT_ISO_READ_UNCOMMITTED ? "read-uncommitted" :
                                                                  "snapshot")));
        WT_ERR(__wt_msg(session, "  Transaction:"));
        WT_ERR(__wt_verbose_dump_txn_one(session, dump_session, 0, NULL));
    } else {
        WT_ERR(__wt_msg(session, "  Number of positioned cursors: %u", dump_session->ncursors));
        TAILQ_FOREACH (cursor, &dump_session->cursors, q) {
            WT_ERR(__wt_msg(session, "Cursor @ %p:", (void *)cursor));
            WT_ERR(__wt_msg(session, "  URI: %s, Internal URI: %s",
              cursor->uri == NULL ? "EMPTY" : cursor->uri,
              cursor->internal_uri == NULL ? "EMPTY" : cursor->internal_uri));
            if (F_ISSET(cursor, WT_CURSTD_OPEN)) {
                WT_ERR(__wt_buf_fmt(session, buf, "OPEN"));
                if (F_ISSET(cursor, WT_CURSTD_KEY_SET) || F_ISSET(cursor, WT_CURSTD_VALUE_SET))
                    WT_ERR(__wt_buf_catfmt(session, buf, ", POSITIONED"));
                else
                    WT_ERR(__wt_buf_catfmt(session, buf, ", RESET"));
                if (F_ISSET(cursor, WT_CURSTD_APPEND))
                    WT_ERR(__wt_buf_catfmt(session, buf, ", APPEND"));
                if (F_ISSET(cursor, WT_CURSTD_BULK))
                    WT_ERR(__wt_buf_catfmt(session, buf, ", BULK"));
                if (F_ISSET(cursor, WT_CURSTD_META_INUSE))
                    WT_ERR(__wt_buf_catfmt(session, buf, ", META_INUSE"));
                if (F_ISSET(cursor, WT_CURSTD_OVERWRITE))
                    WT_ERR(__wt_buf_catfmt(session, buf, ", OVERWRITE"));
                WT_ERR(__wt_msg(session, "  %s", (const char *)buf->data));
            }
            WT_ERR(__wt_msg(session, "  Flags: 0x%" PRIx64, cursor->flags));
            WT_ERR(__wt_msg(session, "  Key_format: %s, Value_format: %s",
              cursor->key_format == NULL ? "EMPTY" : cursor->key_format,
              cursor->value_format == NULL ? "EMPTY" : cursor->value_format));
        }
    }
err:
    __wt_scr_free(session, &buf);
    return (ret);
}

/*
 * __wt_session_set_last_error --
 *     Stores information about the last error to occur during this session.
 */
int
__wt_session_set_last_error(
  WT_SESSION_IMPL *session, int err, int sub_level_err, const char *fmt, ...)
{
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    const char *err_msg_content;
    WT_ERROR_INFO *err_info = &(session->err_info);

    /* Ensure arguments are valid. */
    WT_ASSERT(session, __wt_is_valid_sub_level_error(sub_level_err));
    WT_ASSERT(session, fmt != NULL);

    /*
     * Only update the error struct if an error occurs during a session API call, or if the error
     * struct is being initialized.
     */
    if (!F_ISSET(session, WT_SESSION_SAVE_ERRORS))
        return (0);

    /* Format the error message string. */
    WT_RET(__wt_scr_alloc(session, 0, &buf));
    WT_VA_ARGS_BUF_FORMAT(session, buf, fmt, false);
    err_msg_content = buf->data;

    /* Only set the error if it results in a change. */
    if (err_info->err == err && err_info->sub_level_err == sub_level_err &&
      err_info->err_msg != NULL && strcmp(err_info->err_msg, err_msg_content) == 0)
        goto err;

    /* Free the last error message string. */
    __wt_free(session, err_info->err_msg);

    /* Load error codes and message content into err_info. */
    WT_ERR(__wt_calloc(session, buf->size + 1, 1, &(err_info->err_msg)));
    WT_ERR(__wt_snprintf(err_info->err_msg, buf->size + 1, "%s", err_msg_content));
    err_info->err = err;
    err_info->sub_level_err = sub_level_err;

err:
    __wt_scr_free(session, &buf);
    return (ret);
}
