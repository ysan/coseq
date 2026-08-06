/*
 * main.cpp --- coseq testpp (assert ベースのテスト)
 *
 * 各機能(echo / chain / request_timeout / fan-out / gather / notify)を、
 * シーケンス内で assert しつつ結果を reply し、main 側でも assert で確認する。
 * カバレッジは WITH_COVERAGE=1 でビルドし、実行後に `make gcov`(ルート)で取得。
 */
#include <cassert>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>

#include "coseqpp.h"

enum { MOD_A = 0, MOD_B, MOD_C };
enum { A_ECHO = 0, A_CHAIN, A_REQTO, A_FANOUT, A_GATHER, A_SUBSCRIBE, A_LOCK, A_NOLOCK, A_PROBE, A_GETLOG, A_REQMSG, A_DO_SEND };
enum { B_CHAIN = 0, B_SLOW, B_ECHO };
enum { C_WORK = 0, C_REGISTER, C_NOTIFY_ONCE, C_UNREGISTER, C_NOTIFY_EMPTY, C_SINK };
enum { CAT = 0 };

static std::atomic<int> g_notify_count{0};
static std::atomic<int> g_notify_last_len{-1};   /* 直近 notify のメッセージ長 */
static std::atomic<int> g_log_errors{0};         /* ログCBで拾った ERROR 件数 */
static std::atomic<int> g_sink_count{0};         /* coseq_send の到達回数 */
static std::string      g_log;   /* lock テストの実行順記録(ModuleA スレッドのみが触る) */

static uint8_t *U8 (const std::string &s) { return reinterpret_cast<uint8_t *>(const_cast<char *>(s.c_str())); }

/*=== Module A(テスト対象シーケンス群) ===*/
class module_a : public coseq::module_base {
public:
	module_a (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		std::vector<coseq::sequence_t> s;
		s.push_back({[&](coseq::iface *p){ echo(p); },      "echo"});
		s.push_back({[&](coseq::iface *p){ chain(p); },     "chain"});
		s.push_back({[&](coseq::iface *p){ reqto(p); },     "reqto"});
		s.push_back({[&](coseq::iface *p){ fanout(p); },    "fanout"});
		s.push_back({[&](coseq::iface *p){ gather(p); },    "gather"});
		s.push_back({[&](coseq::iface *p){ subscribe(p); }, "subscribe"});
		s.push_back({[&](coseq::iface *p){ lock_seq(p); },  "lock"});
		s.push_back({[&](coseq::iface *p){ nolock_seq(p); },"nolock"});
		s.push_back({[&](coseq::iface *p){ probe(p); },     "probe"});
		s.push_back({[&](coseq::iface *p){ getlog(p); },    "getlog"});
		s.push_back({[&](coseq::iface *p){ reqmsg(p); },    "reqmsg"});
		s.push_back({[&](coseq::iface *p){ do_send(p); },   "do_send"});
		set_sequences(s);
	}
	virtual ~module_a (void) { reset_sequences(); }

private:
	void echo (coseq::iface *p) {
		assert (std::string(p->get_sequence_name()) == "echo");   // get_sequence_name 検証
		coseq::source &s = p->get_source();
		p->reply(coseq::result::success, s.get_message().data(), s.get_message().length());
	}
	void chain (coseq::iface *p) {
		coseq::source &r = p->request(MOD_B, B_CHAIN);
		assert (r.get_result() == coseq::result::success);
		p->reply(coseq::result::success);
	}
	void reqto (coseq::iface *p) {
		coseq::source &r = p->request_timeout(MOD_B, B_SLOW, 100);   // B は 300ms
		assert (r.get_result() == coseq::result::request_timeout);
		p->reply(coseq::result::success);
	}
	void fanout (coseq::iface *p) {
		uint32_t id_b = p->request_async(MOD_B, B_SLOW);   // 300ms
		uint32_t id_c = p->request_async(MOD_C, C_WORK);   // 100ms
		int got_b = 0, got_c = 0;
		for (int i = 0; i < 2; i++) {
			coseq::source &r = p->wait_reply();
			assert (r.get_result() == coseq::result::success);
			if (r.get_request_id() == id_b) got_b++;
			else if (r.get_request_id() == id_c) got_c++;
			else assert (false && "unknown req_id");
		}
		assert (got_b == 1 && got_c == 1);
		p->reply(coseq::result::success);
	}
	void gather (coseq::iface *p) {
		p->request_async(MOD_B, B_SLOW);
		p->request_async(MOD_C, C_WORK);
		p->request_async(MOD_C, C_WORK);
		std::vector<coseq::reply> replies = p->gather_all();
		assert (replies.size() == 3);
		for (const coseq::reply &r : replies)
			assert (r.rslt == coseq::result::success);
		p->reply(coseq::result::success);
	}
	void subscribe (coseq::iface *p) {
		coseq::source &r = p->request(MOD_C, C_REGISTER);
		assert (r.get_result() == coseq::result::success);
		assert (r.get_message().length() == 1);   // client_id 1byte
		uint8_t cid = r.get_message().data()[0];
		p->reply(coseq::result::success, &cid, sizeof(cid));   // client_id を呼び元へ返す
	}

	// lock 版: ロックしたまま B::slow(300ms) を待つ。待機中に来る probe は
	//          unlock まで保留され、"[" と "]" の後に "P" が来るはず。
	void lock_seq (coseq::iface *p) {
		g_log.clear();
		g_log += "[";
		p->lock();
		p->request(MOD_B, B_SLOW);   // ロック中に yield して待機
		p->unlock();
		g_log += "]";
		p->reply(coseq::result::success);
	}
	// 非 lock 版(対比): 待機中に probe が割り込んで走るので "[P]" になるはず。
	void nolock_seq (coseq::iface *p) {
		g_log.clear();
		g_log += "[";
		p->request(MOD_B, B_SLOW);
		g_log += "]";
		p->reply(coseq::result::success);
	}
	// 割り込みプローブ: 走ったら "P" を記録
	void probe (coseq::iface *p) {
		g_log += "P";
		p->reply(coseq::result::success);
	}
	// 記録した実行順を返信で取り出す
	void getlog (coseq::iface *p) {
		p->reply(coseq::result::success, U8(g_log), g_log.length());
	}
	// in-sequence の request にメッセージを載せる。B::echo が源情報も検証する。
	void reqmsg (coseq::iface *p) {
		std::string msg = "xyz";
		coseq::source &r = p->request(MOD_B, B_ECHO, U8(msg), msg.length());
		assert (r.get_result() == coseq::result::success);
		assert (std::string(reinterpret_cast<char *>(r.get_message().data()), r.get_message().length()) == "xyz");
		p->reply(coseq::result::success);
	}
	// fire-and-forget 送信(返信を待たない)。C::sink へ送るだけ。
	void do_send (coseq::iface *p) {
		p->send(MOD_C, C_SINK);
		p->reply(coseq::result::success);
	}

	void on_receive_notify (coseq::iface *p) override {
		coseq::source &s = p->get_source();
		if (s.get_message().length() > 0)   // 空 notify(引数なし)も許容
			assert (std::string(reinterpret_cast<char *>(s.get_message().data()), s.get_message().length()) == "hello");
		g_notify_count.fetch_add(1);
		g_notify_last_len.store((int)s.get_message().length());
	}
};

/*=== Module B ===*/
class module_b : public coseq::module_base {
public:
	module_b (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		// 配列 + 個数の overload(v1 互換)を使う
		coseq::sequence_t seqs[] = {
			{[&](coseq::iface *p){ chain(p); }, "chain"},
			{[&](coseq::iface *p){ slow(p); },  "slow"},
			{[&](coseq::iface *p){ echo(p); },  "echo"},
		};
		set_sequences(seqs, sizeof(seqs) / sizeof(seqs[0]));
	}
	virtual ~module_b (void) { reset_sequences(); }

private:
	void chain (coseq::iface *p) {
		coseq::source &r = p->request(MOD_C, C_WORK);
		assert (r.get_result() == coseq::result::success);
		p->reply(coseq::result::success);
	}
	void slow (coseq::iface *p) {
		p->wait_timeout(300);
		p->reply(coseq::result::success);
	}
	// 受信メッセージをそのまま返す。源情報(要求元 idx / 実行中 seq idx)も検証。
	void echo (coseq::iface *p) {
		coseq::source &s = p->get_source();
		assert (s.get_thread_idx() == MOD_A);      // 要求元は ModuleA
		assert (s.get_sequence_idx() == B_ECHO);   // 実行中の seq は B_ECHO
		p->reply(coseq::result::success, s.get_message().data(), s.get_message().length());
	}
};

/*=== Module C ===*/
class module_c : public coseq::module_base {
public:
	module_c (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		std::vector<coseq::sequence_t> s;
		s.push_back({[&](coseq::iface *p){ work(p); },        "work"});
		s.push_back({[&](coseq::iface *p){ regist(p); },      "register"});
		s.push_back({[&](coseq::iface *p){ notify_once(p); }, "notify_once"});
		s.push_back({[&](coseq::iface *p){ unregist(p); },    "unregister"});
		s.push_back({[&](coseq::iface *p){ notify_empty(p); },"notify_empty"});
		s.push_back({[&](coseq::iface *p){ sink(p); },        "sink"});
		set_sequences(s);
	}
	virtual ~module_c (void) { reset_sequences(); }

private:
	void work (coseq::iface *p) {
		p->wait_timeout(100);
		p->reply(coseq::result::success);
	}
	void regist (coseq::iface *p) {
		uint8_t client_id = 0;
		if (p->reg_notify(CAT, &client_id)) {
			p->reply(coseq::result::success, &client_id, sizeof(client_id));
		} else {
			// EXTERNAL から呼ばれた等で登録不可 -> error 返信
			p->reply(coseq::result::error);
		}
	}
	void notify_once (coseq::iface *p) {
		std::string msg = "hello";
		p->notify(CAT, U8(msg), msg.length());   // 登録者0でも valid(unreg後の確認に使う)
		p->reply(coseq::result::success);
	}
	// 要求メッセージの client_id で登録解除
	void unregist (coseq::iface *p) {
		uint8_t client_id = *p->get_source().get_message().data();
		bool ok = p->unreg_notify(CAT, client_id);
		p->reply(ok ? coseq::result::success : coseq::result::error);
	}
	// 引数なし notify(メッセージ無し)
	void notify_empty (coseq::iface *p) {
		p->notify(CAT);
		p->reply(coseq::result::success);
	}
	// fire-and-forget の受け先。返信しない(coseq_send の相手)。
	void sink (coseq::iface * /*p*/) {
		g_sink_count.fetch_add(1);
	}
};

#define CHECK(cond) do { if (!(cond)) { std::cerr << "FAIL: " #cond " (line " << __LINE__ << ")" << std::endl; std::abort(); } } while (0)

int main (void) {
	// ログCBを注入(setup 前)。ERROR が来たら数える → 最後に 0 を確認。
	coseq::set_log_cb([](coseq::log_level lv, const std::string &msg) {
		if (lv == coseq::log_level::error) {
			g_log_errors.fetch_add(1);
			std::cerr << "coseq ERROR: " << msg << std::endl;
		}
	});

	coseq::manager mgr;

	auto a = std::make_shared<module_a>("ModuleA", 10);
	auto b = std::make_shared<module_b>("ModuleB", 10);
	auto c = std::make_shared<module_c>("ModuleC", 10);
	std::vector<std::shared_ptr<coseq::module_base>> mods{ a, b, c };
	CHECK (mgr.setup(mods));

	// 1) echo
	{
		std::string msg = "ping";
		coseq::source r = mgr.request_sync(MOD_A, A_ECHO, U8(msg), msg.length());
		CHECK (r.get_result() == coseq::result::success);
		CHECK (std::string(reinterpret_cast<char *>(r.get_message().data()), r.get_message().length()) == "ping");
	}
	// 2) chain A->B->C
	{
		coseq::source r = mgr.request_sync(MOD_A, A_CHAIN);
		CHECK (r.get_result() == coseq::result::success);
	}
	// 3) request_timeout
	{
		coseq::source r = mgr.request_sync(MOD_A, A_REQTO);
		CHECK (r.get_result() == coseq::result::success);   // 内部で REQ_TIMEOUT を確認済み
	}
	// 4) fan-out (wait_reply)
	{
		coseq::source r = mgr.request_sync(MOD_A, A_FANOUT);
		CHECK (r.get_result() == coseq::result::success);
	}
	// 5) gather (wait all)
	{
		coseq::source r = mgr.request_sync(MOD_A, A_GATHER);
		CHECK (r.get_result() == coseq::result::success);
	}
	// 6) notify (subscribe -> notify_once -> recv_notify -> unregister -> 再notifyは届かない)
	{
		coseq::source r = mgr.request_sync(MOD_A, A_SUBSCRIBE);
		CHECK (r.get_result() == coseq::result::success);
		CHECK (r.get_message().length() == 1);
		uint8_t client_id = r.get_message().data()[0];

		mgr.request_sync(MOD_C, C_NOTIFY_ONCE);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		CHECK (g_notify_count.load() == 1);
		CHECK (g_notify_last_len.load() == 5);   // "hello"

		// 引数なし notify(メッセージ長 0) も届く
		mgr.request_sync(MOD_C, C_NOTIFY_EMPTY);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		CHECK (g_notify_count.load() == 2);
		CHECK (g_notify_last_len.load() == 0);

		// 登録解除 -> 以後 notify は届かない
		uint8_t m = client_id;
		coseq::source ur = mgr.request_sync(MOD_C, C_UNREGISTER, &m, sizeof(m));
		CHECK (ur.get_result() == coseq::result::success);
		mgr.request_sync(MOD_C, C_NOTIFY_ONCE);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		CHECK (g_notify_count.load() == 2);   // 解除済みなので増えない
	}
	// 9) in-sequence request + メッセージ + 源情報(get_thread_idx/get_sequence_idx)
	{
		coseq::source r = mgr.request_sync(MOD_A, A_REQMSG);
		CHECK (r.get_result() == coseq::result::success);
	}
	// 7) lock: ロック中は待機中でも probe が保留され、unlock 後に走る -> "[]P"
	{
		mgr.request_async(MOD_A, A_LOCK);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));   // ロック&待機に入るのを待つ
		mgr.request_async(MOD_A, A_PROBE);                            // ロック中に到着 -> 保留
		std::this_thread::sleep_for(std::chrono::milliseconds(400));  // B(300ms)完了まで
		coseq::source r = mgr.request_sync(MOD_A, A_GETLOG);
		std::string log(reinterpret_cast<char *>(r.get_message().data()), r.get_message().length());
		CHECK (log == "[]P");
	}
	// 8) 非lock(対比): probe が待機中に割り込んで走る -> "[P]"
	{
		mgr.request_async(MOD_A, A_NOLOCK);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		mgr.request_async(MOD_A, A_PROBE);
		std::this_thread::sleep_for(std::chrono::milliseconds(400));
		coseq::source r = mgr.request_sync(MOD_A, A_GETLOG);
		std::string log(reinterpret_cast<char *>(r.get_message().data()), r.get_message().length());
		CHECK (log == "[P]");
	}

	// 9b) coseq_send (fire-and-forget): in-seq send + external send -> sink が2回呼ばれる
	{
		int before = g_sink_count.load();
		mgr.request_sync(MOD_A, A_DO_SEND);   // A が C::sink へ send し、自身は success 返信
		mgr.request_async(MOD_C, C_SINK);      // 外部から直接 fire-and-forget
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		CHECK (g_sink_count.load() == before + 2);
	}

	// 正常系(1〜9b)では ERROR ログが出ていないこと
	CHECK (g_log_errors.load() == 0);

	// 10) エラー経路(通常APIで踏めるものだけ)
	{
		int before = g_log_errors.load();
		uint8_t junk[4] = { 1, 2, 3, 4 };
		mgr.request_async(99, 0, junk, sizeof(junk));   // post: invalid module_idx (ERROR, 同期)
		mgr.request_async(MOD_C, 99, nullptr, 0);        // START: invalid seq_idx (ERROR, 非同期)
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		CHECK (g_log_errors.load() == before + 2);       // 2件の ERROR が記録される

		// reg_notify を EXTERNAL から呼ぶ -> WARN + false -> error 返信
		coseq::source r = mgr.request_sync(MOD_C, C_REGISTER);
		CHECK (r.get_result() == coseq::result::error);
	}

	mgr.teardown();

	std::cout << "ALL TESTS PASSED" << std::endl;
	return 0;
}
