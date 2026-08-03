/*
 * sample.c --- coseq (fiber) の C サンプル
 *
 * section_id / set_sectid は一切登場しない。各シーケンスは一直線に書く。
 * echo / chain(+lock) / subscribe / request_timeout / notify / fan-out / gather を実演。
 */
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE   /* usleep() 用 */
#endif

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>

#include "coseq_if.h"

enum { MOD_A = 0, MOD_B, MOD_C };

/* Module A の seq */
enum { A_ECHO = 0, A_CHAIN, A_SUB, A_REQTO, A_FANOUT, A_GATHER };
/* Module B の seq */
enum { B_CHAIN = 0, B_SLOW };
/* Module C の seq */
enum { C_LOOP = 0, C_REGISTER, C_WORK };

enum { NOTIFY_CAT_1 = 0 };

/*=== Module A ===*/
/* 1-shot echo: 受信メッセージをそのまま返信 */
static void a_echo (coseq_if_t *p) {
	coseq_src_t *s = coseq_source(p);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, s->msg.msg, s->msg.size);
}
/* req -> wait -> reply の連鎖(一直線)。lock で待機中の他シーケンスを抑止 */
static void a_chain (coseq_if_t *p) {
	printf("[A::chain] lock + request B::chain\n");
	coseq_lock(p);
	coseq_src_t *r = coseq_request(p, MOD_B, B_CHAIN, NULL, 0);
	coseq_unlock(p);
	printf("[A::chain] reply from B [%d]\n", (int)r->result);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
/* C の notify を購読(reg_notify を C に依頼) */
static void a_sub (coseq_if_t *p) {
	printf("[A::sub] request C::register (subscribe NOTIFY_CAT_1)\n");
	coseq_src_t *r = coseq_request(p, MOD_C, C_REGISTER, NULL, 0);
	uint8_t client_id = (r->msg.size > 0) ? r->msg.msg[0] : 0xff;
	printf("[A::sub] subscribed. client_id=%u\n", client_id);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
/* request_timeout: B::slow(300ms) を 100ms で打ち切り */
static void a_reqto (coseq_if_t *p) {
	printf("[A::reqto] request_timeout B::slow (timeout 100ms, B takes 300ms)\n");
	coseq_src_t *r = coseq_request_timeout(p, MOD_B, B_SLOW, NULL, 0, 100);
	printf("[A::reqto] result [%d] (%s)\n", (int)r->result,
	       r->result == COSEQ_RSLT_REQ_TIMEOUT ? "REQ_TIMEOUT" : "other");
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
/* fan-out: B と C へ並行に投げ、到着順に1件ずつ回収(req_id 突き合わせ) */
static void a_fanout (coseq_if_t *p) {
	uint32_t id_b = coseq_request_async(p, MOD_B, B_SLOW, NULL, 0);   /* 300ms */
	uint32_t id_c = coseq_request_async(p, MOD_C, C_WORK, NULL, 0);   /* 100ms */
	printf("[A::fanout] fired B(id=%u) & C(id=%u), gather...\n", id_b, id_c);
	for (int i = 0; i < 2; i++) {
		coseq_src_t *r = coseq_wait_reply(p);
		const char *who = (r->req_id == id_b) ? "B::slow" : (r->req_id == id_c) ? "C::work" : "?";
		printf("[A::fanout] got id=%u (%s) result=%d\n", r->req_id, who, (int)r->result);
	}
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
/* gather: 全部揃うまで待つ(コールバック版) */
static void gather_cb (coseq_src_t *r, void *user) {
	int *cnt = (int *)user;
	(*cnt)++;
	printf("[A::gather] reply id=%u result=%d\n", r->req_id, (int)r->result);
}
static void a_gather (coseq_if_t *p) {
	coseq_request_async(p, MOD_B, B_SLOW, NULL, 0);   /* 300ms */
	coseq_request_async(p, MOD_C, C_WORK, NULL, 0);   /* 100ms */
	int cnt = 0;
	int n = coseq_gather(p, gather_cb, &cnt);          /* 全部揃うまで待つ */
	printf("[A::gather] all %d replies gathered\n", n);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
/* notify 受信ハンドラ */
static void a_recv_notify (coseq_if_t *p) {
	coseq_src_t *s = coseq_source(p);
	printf("    [A::recv_notify] client_id=%u msg=[%.*s]\n",
	       s->client_id, (int)s->msg.size, (char *)s->msg.msg);
}

/*=== Module B ===*/
static void b_chain (coseq_if_t *p) {
	printf("  [B::chain] request C::work\n");
	coseq_src_t *r = coseq_request(p, MOD_C, C_WORK, NULL, 0);
	printf("  [B::chain] reply from C [%d]\n", (int)r->result);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}
static void b_slow (coseq_if_t *p) {
	coseq_wait_timeout(p, 300);   /* わざと遅い */
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}

/*=== Module C ===*/
/* 「上に戻る」ループ = 本物の for。ローカル変数 n が周回をまたいで保持される */
static void c_loop (coseq_if_t *p) {
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);   /* ループ前に一度だけ返信 */
	int n = 0;
	for (;;) {
		coseq_wait_timeout(p, 300);
		char msg[64];
		int len = snprintf(msg, sizeof(msg), "notify #%d", ++n);
		coseq_notify(p, NOTIFY_CAT_1, (uint8_t *)msg, (size_t)len);
	}
}
static void c_register (coseq_if_t *p) {
	uint8_t client_id = 0;
	bool ok = coseq_reg_notify(p, NOTIFY_CAT_1, &client_id);
	if (ok) coseq_reply(p, COSEQ_RSLT_SUCCESS, &client_id, sizeof(client_id));
	else    coseq_reply(p, COSEQ_RSLT_ERROR, NULL, 0);
}
static void c_work (coseq_if_t *p) {
	coseq_wait_timeout(p, 100);
	coseq_reply(p, COSEQ_RSLT_SUCCESS, NULL, 0);
}

/*=== 登録テーブル ===*/
static const coseq_seq_t a_seqs[] = {
	{a_echo,  "echo"}, {a_chain, "chain"}, {a_sub, "sub"}, {a_reqto, "reqto"},
	{a_fanout, "fanout"}, {a_gather, "gather"},
};
static const coseq_seq_t b_seqs[] = {
	{b_chain, "chain"}, {b_slow, "slow"},
};
static const coseq_seq_t c_seqs[] = {
	{c_loop, "loop"}, {c_register, "register"}, {c_work, "work"},
};

static const coseq_reg_t tbl[] = {
	{ "ModuleA", 10, a_seqs, 6, a_recv_notify },  /* A は notify を受信する */
	{ "ModuleB", 10, b_seqs, 2, NULL },
	{ "ModuleC", 10, c_seqs, 3, NULL },
};

/* ログCB(注入): info/warn/error を標準出力へ */
static int sample_log (int level, const char *fmt, ...) {
	const char *lv = (level == COSEQ_LOG_ERROR) ? "E"
	               : (level == COSEQ_LOG_WARN)  ? "W"
	               : (level == COSEQ_LOG_INFO)  ? "I" : "D";
	va_list ap;
	va_start(ap, fmt);
	printf("  [coseq %s] ", lv);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
	return 0;
}

int main (void) {
	coseq_set_log_cb(sample_log);   /* setup 前に注入 */

	coseq_ctx_if_t *ctx = create_coseq();
	ctx->setup(ctx, tbl, 3);

	/* 1) echo */
	{
		printf("=== 1) echo ===\n");
		char msg[] = "test-message";
		coseq_src_t *r = ctx->request_sync(ctx, MOD_A, A_ECHO, (uint8_t *)msg, strlen(msg));
		printf("reply [%d] [%.*s]\n", (int)r->result, (int)r->msg.size, (char *)r->msg.msg);
	}

	/* 2) chain (+lock): A::chain -> B::chain -> C::work */
	{
		printf("\n=== 2) chain (+lock): A -> B -> C ===\n");
		coseq_src_t *r = ctx->request_sync(ctx, MOD_A, A_CHAIN, NULL, 0);
		printf("reply [%d]\n", (int)r->result);
	}

	/* 3) subscribe: A が C の NOTIFY_CAT_1 を購読 */
	{
		printf("\n=== 3) subscribe ===\n");
		ctx->request_sync(ctx, MOD_A, A_SUB, NULL, 0);
	}

	/* 4) request_timeout */
	{
		printf("\n=== 4) request_timeout ===\n");
		ctx->request_sync(ctx, MOD_A, A_REQTO, NULL, 0);
	}

	/* 5) notify: C のループを起動 -> 300ms ごとに A へ通知(~3回) */
	{
		printf("\n=== 5) notify loop (~3 ticks) ===\n");
		ctx->request_async(ctx, MOD_C, C_LOOP, NULL, 0);
		usleep(1000 * 1000);
	}

	/* 6) fan-out: 並行 request → 到着順に1件ずつ回収 */
	{
		printf("\n=== 6) fan-out (wait_reply per reply) ===\n");
		ctx->request_sync(ctx, MOD_A, A_FANOUT, NULL, 0);
	}

	/* 7) gather: 全部揃うまで待つ */
	{
		printf("\n=== 7) gather (wait all) ===\n");
		ctx->request_sync(ctx, MOD_A, A_GATHER, NULL, 0);
	}

	printf("\n=== teardown ===\n");
	ctx->teardown(ctx);
	ctx->destroy(ctx);
	printf("done\n");
	return 0;
}
