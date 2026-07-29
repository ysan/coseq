/*
 * coseq.c --- coseq コア (C / ucontext fiber スケジューラ / インスタンス形式)
 *
 * モデル:
 *   - インスタンス = create_coseq() が返すハンドル。グローバル状態を持たない。
 *   - モジュール = 1 pthread + 1 スケジューラ。各モジュールは所属インスタンス(mgr)を指す。
 *   - 実行中シーケンス = 1 fiber(専用スタック)。wait 点で swapcontext してスケジューラへ戻る。
 *   - モジュール間通信 = 相手の inbox(mutex+cond)へイベントを post するだけ。
 *   - スケジューラ固有データ(fibers/pending/notify_clients/...)は所有スレッドのみが触る => ロック不要。
 */
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700   /* ucontext 用 */
#endif

#include <ucontext.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "coseq_if.h"

#define STACK_SIZE  (256 * 1024)
#define EXTERNAL    (-1)
#define MAX_MODULE  32

/*=== 前方宣言 ===*/
struct module;
struct fiber;
struct coseq_ctx_private;

/* シーケンスに渡す不透明インタフェースの実体 */
struct coseq_if {
	struct fiber *f;
};

/*=== イベント(モジュール間) ===*/
typedef enum { EV_START, EV_REPLY, EV_NOTIFY } ev_type_t;

typedef struct ev {
	ev_type_t      type;
	uint8_t        seq_idx;    /* START */
	uint32_t       req_id;     /* request/reply 対応付け */
	int            requester;  /* START/NOTIFY: 送り元 module idx or EXTERNAL */
	coseq_result_t result;     /* REPLY */
	uint8_t        category;   /* NOTIFY */
	uint8_t        client_id;  /* NOTIFY */
	uint8_t       *msg;        /* heap コピー(所有) */
	size_t         msg_size;
	struct ev     *next;
} ev_t;

/*=== 到着済み未消費の返信(fan-out/gather 用) ===*/
typedef struct reply_rec {
	uint32_t          req_id;
	coseq_result_t    result;
	uint8_t          *msg;
	size_t            size;
	struct reply_rec *next;
} reply_rec_t;

/*=== fiber(実行中シーケンス) ===*/
typedef struct fiber {
	ucontext_t      ctx;
	char           *stack;
	struct module  *mod;
	coseq_if_t      iface;

	coseq_seq_fn    fn;            /* 実行するシーケンス */
	uint8_t         seq_idx;
	int             reply_to;      /* 返信先 module idx or EXTERNAL */
	uint32_t        reply_req_id;

	coseq_src_t     src;           /* coseq_source() が返す内容 */
	uint8_t        *src_buf;       /* src.msg.msg の backing */
	size_t          src_cap;

	bool            waiting_reply; /* ブロッキング request の返信待ち */
	uint32_t        wait_req_id;   /* 返信待ちの req_id(タイムアウト時に pending を消す) */
	bool            waiting_timer; /* タイマ待ち */
	struct timespec deadline;

	/* async request(fan-out/gather) 用 */
	bool            waiting_any;       /* coseq_wait_reply で待機中 */
	int             async_outstanding; /* 未消費の async 返信数 */
	reply_rec_t    *rq_head, *rq_tail; /* 到着済み未消費の返信キュー */

	bool            done;
	struct fiber   *next;          /* alive fibers 連結 */
} fiber_t;

/*=== 返信待ちエントリ ===*/
typedef struct pending {
	uint32_t        req_id;
	fiber_t        *f;
	struct pending *next;
} pending_t;

/*=== notify listener エントリ ===*/
typedef struct notify_client {
	uint8_t               category;
	int                   listener_module;
	uint8_t               client_id;
	struct notify_client *next;
} notify_client_t;

/*=== モジュール(=1スレッド+スケジューラ) ===*/
typedef struct module {
	int               idx;
	char              name[16];
	uint8_t           que_max;
	const coseq_seq_t *seqs;
	uint8_t           nr_seq;
	coseq_seq_fn      recv_notify;
	void             *user;                    /* ユーザデータ(coseq_self_user) */
	struct coseq_ctx_private *mgr;             /* 所属インスタンス */

	pthread_t         th;
	pthread_mutex_t   mtx;    /* inbox / stop 保護 */
	pthread_cond_t    cv;
	ev_t             *inbox_head, *inbox_tail;
	bool              stop;

	/* --- 以下スケジューラ専用(所有スレッドのみ)。ロック不要 --- */
	ucontext_t        sched_ctx;
	fiber_t          *fibers;
	pending_t        *pending;
	notify_client_t  *notify_clients;
	uint32_t          next_req_id;
	uint8_t           next_client_id;
	fiber_t          *current;
	fiber_t          *locked_by;
	ev_t             *deferred_head, *deferred_tail;
} module_t;

/*=== インスタンス private ===*/
struct coseq_ctx_private {
	module_t         modules[MAX_MODULE];
	int              nr_module;

	/* 外部(非モジュール)スレッド用メールボックス */
	pthread_mutex_t  ext_mtx;
	pthread_cond_t   ext_cv;
	bool             ext_ready;
	uint32_t         ext_wait_rid;
	coseq_result_t   ext_result;
	uint8_t         *ext_msg;
	size_t           ext_size;
	uint32_t         ext_id;
};
typedef struct coseq_ctx_private coseq_ctx_private_t;

/*=== trampoline へ「起動する fiber」を渡す(モジュールスレッドごと) ===*/
static __thread fiber_t *g_start = NULL;

/*=== 時刻ヘルパ(CLOCK_REALTIME: pthread_cond_timedwait 既定) ===*/
static struct timespec now_ts (void) {
	struct timespec t;
	clock_gettime(CLOCK_REALTIME, &t);
	return t;
}
static struct timespec ts_add_ms (struct timespec t, uint32_t ms) {
	t.tv_sec  += ms / 1000;
	t.tv_nsec += (long)(ms % 1000) * 1000000L;
	if (t.tv_nsec >= 1000000000L) { t.tv_sec++; t.tv_nsec -= 1000000000L; }
	return t;
}
static bool ts_lt (const struct timespec *a, const struct timespec *b) {
	return (a->tv_sec < b->tv_sec) ||
	       (a->tv_sec == b->tv_sec && a->tv_nsec < b->tv_nsec);
}

/*=== inbox へ post(唯一のクロススレッド経路) ===*/
static void post (coseq_ctx_private_t *mgr, int module_idx, ev_t *e) {
	module_t *m = &mgr->modules[module_idx];
	e->next = NULL;
	pthread_mutex_lock(&m->mtx);
	if (m->inbox_tail) m->inbox_tail->next = e;
	else               m->inbox_head = e;
	m->inbox_tail = e;
	pthread_cond_signal(&m->cv);
	pthread_mutex_unlock(&m->mtx);
}

static ev_t *make_ev (ev_type_t type, uint8_t seq_idx, uint32_t req_id, int requester,
                      coseq_result_t result, const uint8_t *msg, size_t size) {
	ev_t *e = calloc(1, sizeof(*e));
	e->type = type; e->seq_idx = seq_idx; e->req_id = req_id;
	e->requester = requester; e->result = result;
	if (msg && size) { e->msg = malloc(size); memcpy(e->msg, msg, size); e->msg_size = size; }
	return e;
}

static void defer_ev (module_t *m, ev_t *e) {
	e->next = NULL;
	if (m->deferred_tail) m->deferred_tail->next = e;
	else                  m->deferred_head = e;
	m->deferred_tail = e;
}

/*=== pending 管理(スケジューラ専用) ===*/
static void pending_add (module_t *m, uint32_t req_id, fiber_t *f) {
	pending_t *p = malloc(sizeof(*p));
	p->req_id = req_id; p->f = f; p->next = m->pending; m->pending = p;
}
static fiber_t *pending_peek (module_t *m, uint32_t req_id) {
	for (pending_t *p = m->pending; p; p = p->next)
		if (p->req_id == req_id) return p->f;
	return NULL;
}
static fiber_t *pending_take (module_t *m, uint32_t req_id) {
	pending_t **pp = &m->pending;
	while (*pp) {
		if ((*pp)->req_id == req_id) {
			pending_t *hit = *pp; *pp = hit->next;
			fiber_t *f = hit->f; free(hit); return f;
		}
		pp = &(*pp)->next;
	}
	return NULL;
}

/*=== source 設定 ===*/
static void set_src (fiber_t *f, const uint8_t *msg, size_t size, coseq_result_t r,
                     uint8_t thread_idx, uint8_t seq_idx, uint8_t client_id, uint32_t req_id) {
	if (size > f->src_cap) { f->src_buf = realloc(f->src_buf, size); f->src_cap = size; }
	if (msg && size) memcpy(f->src_buf, msg, size);
	f->src.msg.msg    = f->src_buf;
	f->src.msg.size   = size;
	f->src.result     = r;
	f->src.thread_idx = thread_idx;
	f->src.seq_idx    = seq_idx;
	f->src.client_id  = client_id;
	f->src.req_id     = req_id;
}

/*=== 外部への返信配達 ===*/
static void ext_deliver (coseq_ctx_private_t *mgr, uint32_t req_id, coseq_result_t r,
                         const uint8_t *msg, size_t size) {
	pthread_mutex_lock(&mgr->ext_mtx);
	if (req_id == mgr->ext_wait_rid) {
		mgr->ext_result = r;
		free(mgr->ext_msg); mgr->ext_msg = NULL; mgr->ext_size = 0;
		if (msg && size) { mgr->ext_msg = malloc(size); memcpy(mgr->ext_msg, msg, size); mgr->ext_size = size; }
		mgr->ext_ready = true;
		pthread_cond_signal(&mgr->ext_cv);
	}
	pthread_mutex_unlock(&mgr->ext_mtx);
}

/*=== fiber 起動 trampoline ===*/
static void trampoline (void) {
	fiber_t *f = g_start;
	f->fn(&f->iface);
	f->done = true;
}

static fiber_t *new_fiber (module_t *m, coseq_seq_fn fn, uint8_t seq_idx,
                           int reply_to, uint32_t reply_req_id,
                           const uint8_t *msg, size_t size,
                           coseq_result_t r, uint8_t thread_idx, uint8_t client_id) {
	fiber_t *f = calloc(1, sizeof(*f));
	f->mod = m;
	f->stack = malloc(STACK_SIZE);
	f->iface.f = f;
	f->fn = fn;
	f->seq_idx = seq_idx;
	f->reply_to = reply_to;
	f->reply_req_id = reply_req_id;
	set_src(f, msg, size, r, thread_idx, seq_idx, client_id, reply_req_id);

	getcontext(&f->ctx);
	f->ctx.uc_stack.ss_sp   = f->stack;
	f->ctx.uc_stack.ss_size = STACK_SIZE;
	f->ctx.uc_link          = &m->sched_ctx;
	makecontext(&f->ctx, trampoline, 0);

	f->next = m->fibers; m->fibers = f;
	return f;
}

static void resume (module_t *m, fiber_t *f) {
	f->waiting_timer = false;
	f->waiting_reply = false;
	f->waiting_any   = false;
	m->current = f;
	g_start = f;
	swapcontext(&m->sched_ctx, &f->ctx);
	m->current = NULL;
}

/* EV_REPLY を対象 fiber へ配達: ブロッキング完了 / wait_reply 起床 / キュー投入 */
static void handle_reply (module_t *m, ev_t *e) {
	fiber_t *f = pending_peek(m, e->req_id);
	if (!f) return;                       /* 未知/遅延(タイムアウト後など) */
	pending_take(m, e->req_id);

	if (f->waiting_reply && f->wait_req_id == e->req_id) {
		/* ブロッキング coseq_request の完了 */
		set_src(f, e->msg, e->msg_size, e->result, (uint8_t)e->requester, f->seq_idx, 0, e->req_id);
		resume(m, f);
	} else if (f->waiting_any) {
		/* coseq_wait_reply で待機中 → この返信で起床 */
		set_src(f, e->msg, e->msg_size, e->result, (uint8_t)e->requester, f->seq_idx, 0, e->req_id);
		resume(m, f);
	} else {
		/* fiber は別状態(実行中/別待ち) → キューに積んで後で消費 */
		reply_rec_t *rr = calloc(1, sizeof(*rr));
		rr->req_id = e->req_id;
		rr->result = e->result;
		if (e->msg && e->msg_size) { rr->msg = malloc(e->msg_size); memcpy(rr->msg, e->msg, e->msg_size); rr->size = e->msg_size; }
		if (f->rq_tail) f->rq_tail->next = rr; else f->rq_head = rr;
		f->rq_tail = rr;
	}
}

static void reap_done (module_t *m) {
	fiber_t **pp = &m->fibers;
	while (*pp) {
		fiber_t *f = *pp;
		if (f->done) {
			if (m->locked_by == f) m->locked_by = NULL;
			*pp = f->next;
			free(f->stack); free(f->src_buf); free(f);
		} else {
			pp = &f->next;
		}
	}
}

/*=== スケジューラ本体(モジュールスレッド) ===*/
static void *sched_loop (void *arg) {
	module_t *m = (module_t *)arg;

	for (;;) {
		pthread_mutex_lock(&m->mtx);

		if (!m->locked_by && m->deferred_head) {
			m->deferred_tail->next = m->inbox_head;
			m->inbox_head = m->deferred_head;
			if (!m->inbox_tail) m->inbox_tail = m->deferred_tail;
			m->deferred_head = m->deferred_tail = NULL;
		}

		bool has_timer = false;
		struct timespec nearest = {0, 0};
		for (fiber_t *f = m->fibers; f; f = f->next) {
			if (!f->waiting_timer) continue;
			if (m->locked_by && f != m->locked_by) continue;
			if (!has_timer || ts_lt(&f->deadline, &nearest)) { nearest = f->deadline; has_timer = true; }
		}

		if (!m->inbox_head && !m->stop) {
			if (has_timer) pthread_cond_timedwait(&m->cv, &m->mtx, &nearest);
			else           pthread_cond_wait(&m->cv, &m->mtx);
		}
		if (m->stop) { pthread_mutex_unlock(&m->mtx); break; }

		ev_t *evs = m->inbox_head;
		m->inbox_head = m->inbox_tail = NULL;
		pthread_mutex_unlock(&m->mtx);

		struct timespec now = now_ts();
		for (fiber_t *f = m->fibers; f; ) {
			fiber_t *nx = f->next;
			if (f->waiting_timer && !ts_lt(&now, &f->deadline)) {
				if (!(m->locked_by && f != m->locked_by)) {
					if (f->waiting_reply) {
						pending_take(m, f->wait_req_id);
						set_src(f, NULL, 0, COSEQ_RSLT_REQ_TIMEOUT, 0, f->seq_idx, 0, f->wait_req_id);
					}
					resume(m, f);
				}
			}
			f = nx;
		}

		for (ev_t *e = evs; e; ) {
			ev_t *nx = e->next;
			bool consumed = true;

			if (m->locked_by && e->type != EV_NOTIFY) {
				if (e->type == EV_REPLY && pending_peek(m, e->req_id) == m->locked_by) {
					handle_reply(m, e);
				} else {
					defer_ev(m, e);
					consumed = false;
				}
			} else if (e->type == EV_START) {
				fiber_t *f = new_fiber(m, m->seqs[e->seq_idx].fn, e->seq_idx,
				                       e->requester, e->req_id, e->msg, e->msg_size,
				                       COSEQ_RSLT_IGNORE, (uint8_t)e->requester, 0);
				resume(m, f);
			} else if (e->type == EV_REPLY) {
				handle_reply(m, e);
			} else { /* EV_NOTIFY */
				if (m->recv_notify) {
					fiber_t *f = new_fiber(m, m->recv_notify, 0, EXTERNAL, 0,
					                       e->msg, e->msg_size, COSEQ_RSLT_SUCCESS,
					                       (uint8_t)e->requester, e->client_id);
					resume(m, f);
				}
			}

			if (consumed) { free(e->msg); free(e); }
			e = nx;
		}

		reap_done(m);
	}
	return NULL;
}

/*==================== 公開API: シーケンス内 ====================*/

static coseq_src_t *request_common (coseq_if_t *p_if, uint8_t module_idx, uint8_t seq_idx,
                                    uint8_t *msg, size_t msg_size, bool with_timeout, uint32_t timeout_msec) {
	fiber_t *f = p_if->f;
	module_t *m = f->mod;
	coseq_ctx_private_t *mgr = m->mgr;
	uint32_t rid = m->next_req_id++;
	pending_add(m, rid, f);

	f->waiting_reply = true;
	f->wait_req_id = rid;
	if (with_timeout) { f->waiting_timer = true; f->deadline = ts_add_ms(now_ts(), timeout_msec); }

	post(mgr, module_idx, make_ev(EV_START, seq_idx, rid, m->idx, COSEQ_RSLT_IGNORE, msg, msg_size));

	swapcontext(&f->ctx, &m->sched_ctx);
	return &f->src;
}

coseq_src_t *coseq_request (coseq_if_t *p_if, uint8_t module_idx, uint8_t seq_idx,
                            uint8_t *msg, size_t msg_size) {
	return request_common(p_if, module_idx, seq_idx, msg, msg_size, false, 0);
}

coseq_src_t *coseq_request_timeout (coseq_if_t *p_if, uint8_t module_idx, uint8_t seq_idx,
                                    uint8_t *msg, size_t msg_size, uint32_t timeout_msec) {
	return request_common(p_if, module_idx, seq_idx, msg, msg_size, true, timeout_msec);
}

/* 返信を待たずに投げる。req_id を返す(yield しない)。後で coseq_wait_reply で回収。 */
uint32_t coseq_request_async (coseq_if_t *p_if, uint8_t module_idx, uint8_t seq_idx,
                              uint8_t *msg, size_t msg_size) {
	fiber_t *f = p_if->f;
	module_t *m = f->mod;
	uint32_t rid = m->next_req_id++;
	pending_add(m, rid, f);           /* async: waiting_reply/wait_req_id は設定しない */
	f->async_outstanding++;
	post(m->mgr, module_idx, make_ev(EV_START, seq_idx, rid, m->idx, COSEQ_RSLT_IGNORE, msg, msg_size));
	return rid;
}

/* 未消費の async 返信を1件受け取る(到着順)。req_id で突き合わせる。
 * 待つべき返信が無ければ NULL。 */
coseq_src_t *coseq_wait_reply (coseq_if_t *p_if) {
	fiber_t *f = p_if->f;
	module_t *m = f->mod;
	if (f->async_outstanding == 0 && f->rq_head == NULL) return NULL;

	if (f->rq_head) {                 /* 既に到着済みの返信がある */
		reply_rec_t *rr = f->rq_head;
		f->rq_head = rr->next; if (!f->rq_head) f->rq_tail = NULL;
		set_src(f, rr->msg, rr->size, rr->result, 0, f->seq_idx, 0, rr->req_id);
		free(rr->msg); free(rr);
		f->async_outstanding--;
		return &f->src;
	}

	f->waiting_any = true;
	swapcontext(&f->ctx, &m->sched_ctx);   /* ★yield: いずれかの返信で起床(handle_reply が set_src 済み) */
	f->async_outstanding--;
	return &f->src;
}

/* 未消費の async 返信が全て揃うまで待ち、到着順に cb を呼ぶ。回収件数を返す。 */
int coseq_gather (coseq_if_t *p_if, coseq_reply_cb cb, void *user) {
	int n = 0;
	coseq_src_t *r;
	while ((r = coseq_wait_reply(p_if)) != NULL) {
		if (cb) cb(r, user);
		n++;
	}
	return n;
}

void coseq_reply (coseq_if_t *p_if, coseq_result_t result, uint8_t *msg, size_t msg_size) {
	fiber_t *f = p_if->f;
	coseq_ctx_private_t *mgr = f->mod->mgr;
	if (f->reply_to == EXTERNAL) {
		ext_deliver(mgr, f->reply_req_id, result, msg, msg_size);
	} else {
		post(mgr, f->reply_to, make_ev(EV_REPLY, 0, f->reply_req_id, f->mod->idx, result, msg, msg_size));
	}
}

void coseq_wait_timeout (coseq_if_t *p_if, uint32_t msec) {
	fiber_t *f = p_if->f;
	f->waiting_timer = true;
	f->deadline = ts_add_ms(now_ts(), msec);
	swapcontext(&f->ctx, &f->mod->sched_ctx);
}

coseq_src_t *coseq_source (coseq_if_t *p_if) { return &p_if->f->src; }

uint8_t coseq_self_module (coseq_if_t *p_if) { return (uint8_t)p_if->f->mod->idx; }
uint8_t coseq_self_seq    (coseq_if_t *p_if) { return p_if->f->seq_idx; }
void   *coseq_self_user   (coseq_if_t *p_if) { return p_if->f->mod->user; }

/*--- notify ---*/
bool coseq_reg_notify (coseq_if_t *p_if, uint8_t category, uint8_t *out_client_id) {
	fiber_t *f = p_if->f;
	module_t *m = f->mod;
	if (f->reply_to == EXTERNAL) return false;
	uint8_t id = m->next_client_id++;
	notify_client_t *nc = malloc(sizeof(*nc));
	nc->category = category;
	nc->listener_module = f->reply_to;
	nc->client_id = id;
	nc->next = m->notify_clients;
	m->notify_clients = nc;
	if (out_client_id) *out_client_id = id;
	return true;
}

bool coseq_unreg_notify (coseq_if_t *p_if, uint8_t category, uint8_t client_id) {
	module_t *m = p_if->f->mod;
	notify_client_t **pp = &m->notify_clients;
	while (*pp) {
		if ((*pp)->category == category && (*pp)->client_id == client_id) {
			notify_client_t *hit = *pp; *pp = hit->next; free(hit); return true;
		}
		pp = &(*pp)->next;
	}
	return false;
}

bool coseq_notify (coseq_if_t *p_if, uint8_t category, uint8_t *msg, size_t msg_size) {
	module_t *m = p_if->f->mod;
	coseq_ctx_private_t *mgr = m->mgr;
	bool any = false;
	for (notify_client_t *nc = m->notify_clients; nc; nc = nc->next) {
		if (nc->category != category) continue;
		ev_t *e = make_ev(EV_NOTIFY, 0, 0, m->idx, COSEQ_RSLT_SUCCESS, msg, msg_size);
		e->category = category;
		e->client_id = nc->client_id;
		post(mgr, nc->listener_module, e);
		any = true;
	}
	return any;
}

/*--- lock ---*/
void coseq_lock (coseq_if_t *p_if) {
	p_if->f->mod->locked_by = p_if->f;
}
void coseq_unlock (coseq_if_t *p_if) {
	module_t *m = p_if->f->mod;
	if (m->locked_by == p_if->f) m->locked_by = NULL;
}

/*==================== インスタンスのメソッド ====================*/

static bool         _setup         (coseq_ctx_if_t *self, const coseq_reg_t *tbl, uint8_t nr_tbl);
static void         _teardown      (coseq_ctx_if_t *self);
static coseq_src_t *_request_sync  (coseq_ctx_if_t *self, uint8_t module_idx, uint8_t seq_idx, uint8_t *msg, size_t msg_size);
static void         _request_async (coseq_ctx_if_t *self, uint8_t module_idx, uint8_t seq_idx, uint8_t *msg, size_t msg_size);
static void         _destroy       (coseq_ctx_if_t *self);

static bool _setup (coseq_ctx_if_t *self, const coseq_reg_t *tbl, uint8_t nr_tbl) {
	coseq_ctx_private_t *priv = self->impl;
	if (nr_tbl > MAX_MODULE) return false;
	priv->nr_module = nr_tbl;
	for (uint8_t i = 0; i < nr_tbl; i++) {
		module_t *m = &priv->modules[i];
		memset(m, 0, sizeof(*m));
		m->idx = i;
		m->mgr = priv;
		strncpy(m->name, tbl[i].name ? tbl[i].name : "", sizeof(m->name) - 1);
		m->que_max     = tbl[i].nr_que_max;
		m->seqs        = tbl[i].seq_array;
		m->nr_seq      = tbl[i].nr_seq_max;
		m->recv_notify = tbl[i].recv_notify;
		m->user        = tbl[i].user;
		pthread_mutex_init(&m->mtx, NULL);
		pthread_cond_init(&m->cv, NULL);
		m->next_req_id = 1;
	}
	for (uint8_t i = 0; i < nr_tbl; i++) {
		pthread_create(&priv->modules[i].th, NULL, sched_loop, &priv->modules[i]);
	}
	return true;
}

static void _teardown (coseq_ctx_if_t *self) {
	coseq_ctx_private_t *priv = self->impl;
	if (priv->nr_module == 0) return;   /* 冪等 */

	for (int i = 0; i < priv->nr_module; i++) {
		module_t *m = &priv->modules[i];
		pthread_mutex_lock(&m->mtx);
		m->stop = true;
		pthread_cond_signal(&m->cv);
		pthread_mutex_unlock(&m->mtx);
	}
	for (int i = 0; i < priv->nr_module; i++) {
		pthread_join(priv->modules[i].th, NULL);
	}
	for (int i = 0; i < priv->nr_module; i++) {
		module_t *m = &priv->modules[i];
		for (fiber_t *f = m->fibers; f; ) {
			fiber_t *n = f->next;
			for (reply_rec_t *rr = f->rq_head; rr; ) { reply_rec_t *rn = rr->next; free(rr->msg); free(rr); rr = rn; }
			free(f->stack); free(f->src_buf); free(f);
			f = n;
		}
		for (pending_t *p = m->pending; p; ) { pending_t *n = p->next; free(p); p = n; }
		for (notify_client_t *nc = m->notify_clients; nc; ) { notify_client_t *n = nc->next; free(nc); nc = n; }
		for (ev_t *e = m->inbox_head; e; ) { ev_t *n = e->next; free(e->msg); free(e); e = n; }
		for (ev_t *e = m->deferred_head; e; ) { ev_t *n = e->next; free(e->msg); free(e); e = n; }
		pthread_mutex_destroy(&m->mtx);
		pthread_cond_destroy(&m->cv);
	}
	priv->nr_module = 0;
}

static coseq_src_t *_request_sync (coseq_ctx_if_t *self, uint8_t module_idx, uint8_t seq_idx,
                                   uint8_t *msg, size_t msg_size) {
	coseq_ctx_private_t *priv = self->impl;
	static __thread coseq_src_t s;
	static __thread uint8_t    *buf = NULL;
	static __thread size_t      cap = 0;

	pthread_mutex_lock(&priv->ext_mtx);
	uint32_t rid = priv->ext_id++;
	priv->ext_wait_rid = rid;
	priv->ext_ready = false;
	post(priv, module_idx, make_ev(EV_START, seq_idx, rid, EXTERNAL, COSEQ_RSLT_IGNORE, msg, msg_size));
	while (!priv->ext_ready) pthread_cond_wait(&priv->ext_cv, &priv->ext_mtx);
	priv->ext_wait_rid = (uint32_t)-1;

	if (priv->ext_size > cap) { buf = realloc(buf, priv->ext_size); cap = priv->ext_size; }
	if (priv->ext_size) memcpy(buf, priv->ext_msg, priv->ext_size);
	s.msg.msg = buf; s.msg.size = priv->ext_size;
	s.result = priv->ext_result; s.req_id = rid; s.thread_idx = module_idx; s.seq_idx = seq_idx;
	free(priv->ext_msg); priv->ext_msg = NULL; priv->ext_size = 0;
	pthread_mutex_unlock(&priv->ext_mtx);
	return &s;
}

static void _request_async (coseq_ctx_if_t *self, uint8_t module_idx, uint8_t seq_idx,
                            uint8_t *msg, size_t msg_size) {
	coseq_ctx_private_t *priv = self->impl;
	pthread_mutex_lock(&priv->ext_mtx);
	uint32_t rid = priv->ext_id++;
	pthread_mutex_unlock(&priv->ext_mtx);
	post(priv, module_idx, make_ev(EV_START, seq_idx, rid, EXTERNAL, COSEQ_RSLT_IGNORE, msg, msg_size));
}

static void _destroy (coseq_ctx_if_t *self) {
	if (!self) return;
	_teardown(self);
	pthread_mutex_destroy(&self->impl->ext_mtx);
	pthread_cond_destroy(&self->impl->ext_cv);
	free(self);
}

/*==================== ファクトリ ====================*/

coseq_ctx_if_t *create_coseq (void) {
	void *p = calloc(1, sizeof(coseq_ctx_if_t) + sizeof(coseq_ctx_private_t));
	if (!p) return NULL;

	coseq_ctx_if_t *self = (coseq_ctx_if_t *)p;
	self->setup         = _setup;
	self->teardown      = _teardown;
	self->request_sync  = _request_sync;
	self->request_async = _request_async;
	self->destroy       = _destroy;

	coseq_ctx_private_t *priv = (coseq_ctx_private_t *)(self + 1);
	self->impl = priv;
	pthread_mutex_init(&priv->ext_mtx, NULL);
	pthread_cond_init(&priv->ext_cv, NULL);
	priv->ext_wait_rid = (uint32_t)-1;
	priv->ext_id = 1;

	return self;
}
