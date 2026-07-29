/*
 * sample.cpp --- coseq C++ ラッパーのサンプル
 *
 * section_id / set_section_id は無し。各シーケンスは一直線。
 * echo / chain(+lock) / subscribe / request_timeout / fan-out / gather を実演。
 *
 * フレームワーク側の名前は都度 coseq:: で明示修飾する(using しない)。
 */
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>
#include <cstring>

#include "coseqpp.h"

enum { _MODULE_A = 0, _MODULE_B, _MODULE_C };
enum { A_ECHO = 0, A_CHAIN, A_SUB, A_REQTO, A_FANOUT, A_GATHER };
enum { B_CHAIN = 0, B_SLOW };
enum { C_LOOP = 0, C_REGISTER, C_WORK };
enum { NOTIFY_CAT_1 = 0 };

static uint8_t *U8 (const std::string &s) { return reinterpret_cast<uint8_t *>(const_cast<char *>(s.c_str())); }

/*=== Module A ===*/
class module_a : public coseq::module_base {
public:
	module_a (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		std::vector<coseq::sequence_t> s;
		s.push_back({[&](coseq::iface *p){ echo(p); },   "echo"});
		s.push_back({[&](coseq::iface *p){ chain(p); },  "chain"});
		s.push_back({[&](coseq::iface *p){ sub(p); },    "sub"});
		s.push_back({[&](coseq::iface *p){ reqto(p); },  "reqto"});
		s.push_back({[&](coseq::iface *p){ fanout(p); }, "fanout"});
		s.push_back({[&](coseq::iface *p){ gather(p); }, "gather"});
		set_sequences(s);
	}
	virtual ~module_a (void) { reset_sequences(); }

private:
	void echo (coseq::iface *p) {
		coseq::source &s = p->get_source();
		p->reply(coseq::result::success, s.get_message().data(), s.get_message().length());
	}
	void chain (coseq::iface *p) {
		std::cout << "[A::chain] lock + request B::chain" << std::endl;
		p->lock();
		coseq::source &r = p->request(_MODULE_B, B_CHAIN);
		p->unlock();
		std::cout << "[A::chain] reply from B [" << (int)r.get_result() << "]" << std::endl;
		p->reply(coseq::result::success);
	}
	void sub (coseq::iface *p) {
		std::cout << "[A::sub] request C::register" << std::endl;
		coseq::source &r = p->request(_MODULE_C, C_REGISTER);
		uint8_t client_id = r.get_message().length() ? *r.get_message().data() : 0xff;
		std::cout << "[A::sub] subscribed. client_id=" << (int)client_id << std::endl;
		p->reply(coseq::result::success);
	}
	void reqto (coseq::iface *p) {
		std::cout << "[A::reqto] request_timeout B::slow (100ms, B takes 300ms)" << std::endl;
		coseq::source &r = p->request_timeout(_MODULE_B, B_SLOW, 100);
		std::cout << "[A::reqto] result [" << (int)r.get_result() << "] ("
		          << (r.get_result() == coseq::result::request_timeout ? "REQ_TIMEOUT" : "other") << ")" << std::endl;
		p->reply(coseq::result::success);
	}

	// fan-out: B と C へ並行に投げ、到着順に req_id で突き合わせて1件ずつ回収
	void fanout (coseq::iface *p) {
		uint32_t id_b = p->request_async(_MODULE_B, B_SLOW);   // 300ms
		uint32_t id_c = p->request_async(_MODULE_C, C_WORK);   // 100ms
		std::cout << "[A::fanout] fired B(id=" << id_b << ") & C(id=" << id_c << "), gather..." << std::endl;
		for (int i = 0; i < 2; i++) {
			coseq::source &r = p->wait_reply();
			const char *who = (r.get_request_id() == id_b) ? "B::slow"
			                : (r.get_request_id() == id_c) ? "C::work" : "?";
			std::cout << "[A::fanout] got id=" << r.get_request_id()
			          << " (" << who << ") result=" << (int)r.get_result() << std::endl;
		}
		p->reply(coseq::result::success);
	}

	// gather: 全部揃うまで待つ(vector で受け取る)
	void gather (coseq::iface *p) {
		p->request_async(_MODULE_B, B_SLOW);   // 300ms
		p->request_async(_MODULE_C, C_WORK);   // 100ms
		std::vector<coseq::reply> replies = p->gather_all();
		for (const coseq::reply &r : replies)
			std::cout << "[A::gather] reply id=" << r.req_id << " result=" << (int)r.rslt << std::endl;
		std::cout << "[A::gather] all " << replies.size() << " replies gathered" << std::endl;
		p->reply(coseq::result::success);
	}

	// notify 受信
	void on_receive_notify (coseq::iface *p) override {
		coseq::source &s = p->get_source();
		std::string msg(reinterpret_cast<char *>(s.get_message().data()), s.get_message().length());
		std::cout << "    [A::recv_notify] client_id=" << (int)s.get_client_id()
		          << " msg=[" << msg << "]" << std::endl;
	}
};

/*=== Module B ===*/
class module_b : public coseq::module_base {
public:
	module_b (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		std::vector<coseq::sequence_t> s;
		s.push_back({[&](coseq::iface *p){ chain(p); }, "chain"});
		s.push_back({[&](coseq::iface *p){ slow(p); },  "slow"});
		set_sequences(s);
	}
	virtual ~module_b (void) { reset_sequences(); }

private:
	void chain (coseq::iface *p) {
		std::cout << "  [B::chain] request C::work" << std::endl;
		coseq::source &r = p->request(_MODULE_C, C_WORK);
		std::cout << "  [B::chain] reply from C [" << (int)r.get_result() << "]" << std::endl;
		p->reply(coseq::result::success);
	}
	void slow (coseq::iface *p) {
		p->wait_timeout(300);
		p->reply(coseq::result::success);
	}
};

/*=== Module C ===*/
class module_c : public coseq::module_base {
public:
	module_c (std::string name, uint8_t que_max) : coseq::module_base(std::move(name), que_max) {
		std::vector<coseq::sequence_t> s;
		s.push_back({[&](coseq::iface *p){ loop(p); },   "loop"});
		s.push_back({[&](coseq::iface *p){ regist(p); }, "register"});
		s.push_back({[&](coseq::iface *p){ work(p); },   "work"});
		set_sequences(s);
	}
	virtual ~module_c (void) { reset_sequences(); }

private:
	void loop (coseq::iface *p) {
		p->reply(coseq::result::success);   // ループ前に一度だけ返信
		int n = 0;
		for (;;) {                           // 「上に戻る」= 本物のループ、n は保持
			p->wait_timeout(300);
			std::string msg = "notify #" + std::to_string(++n);
			p->notify(NOTIFY_CAT_1, U8(msg), msg.length());
		}
	}
	void regist (coseq::iface *p) {
		uint8_t client_id = 0;
		if (p->reg_notify(NOTIFY_CAT_1, &client_id))
			p->reply(coseq::result::success, &client_id, sizeof(client_id));
		else
			p->reply(coseq::result::error);
	}
	void work (coseq::iface *p) {
		p->wait_timeout(100);
		p->reply(coseq::result::success);
	}
};

int main (void) {
	coseq::manager mgr;

	auto a = std::make_shared<module_a>("ModuleA", 10);
	auto b = std::make_shared<module_b>("ModuleB", 10);
	auto c = std::make_shared<module_c>("ModuleC", 10);
	std::vector<std::shared_ptr<coseq::module_base>> mods{ a, b, c };
	mgr.setup(mods);

	{
		std::cout << "=== 1) echo ===" << std::endl;
		std::string msg = "test-message";
		coseq::source r = mgr.request_sync(_MODULE_A, A_ECHO, U8(msg), msg.length());
		std::cout << "reply [" << (int)r.get_result() << "] ["
		          << std::string((char *)r.get_message().data(), r.get_message().length()) << "]" << std::endl;
	}
	{
		std::cout << "\n=== 2) chain (+lock): A -> B -> C ===" << std::endl;
		coseq::source r = mgr.request_sync(_MODULE_A, A_CHAIN);
		std::cout << "reply [" << (int)r.get_result() << "]" << std::endl;
	}
	{
		std::cout << "\n=== 3) subscribe ===" << std::endl;
		mgr.request_sync(_MODULE_A, A_SUB);
	}
	{
		std::cout << "\n=== 4) request_timeout ===" << std::endl;
		mgr.request_sync(_MODULE_A, A_REQTO);
	}
	{
		std::cout << "\n=== 5) notify loop (~3 ticks) ===" << std::endl;
		mgr.request_async(_MODULE_C, C_LOOP);
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
	{
		std::cout << "\n=== 6) fan-out (wait_reply per reply) ===" << std::endl;
		mgr.request_sync(_MODULE_A, A_FANOUT);
	}
	{
		std::cout << "\n=== 7) gather (wait all) ===" << std::endl;
		mgr.request_sync(_MODULE_A, A_GATHER);
	}

	std::cout << "\n=== teardown ===" << std::endl;
	mgr.teardown();
	std::cout << "done" << std::endl;
	return 0;
}
