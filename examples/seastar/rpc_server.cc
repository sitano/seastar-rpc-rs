/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

 /*
 * Copyright 2015 Cloudius Systems
 * Modified by Ivan Prisiazhnyi <john.koepi@gmail.com>
 */

#include <cmath>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/log.hh>

#include "rpc_serializer.hh"
#include "stop_signal.hh"

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

static logger lg("rpc_server");
static mycomp mc;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
                    ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
                    ("compress", bpo::value<bool>()->default_value(false), "Compress RPC traffic");

    return app.run(ac, av, [&] {
        return async([&app] {
            seastar_apps_lib::stop_signal stop_signal;
            
            auto&& config = app.configuration();
            uint16_t port = config["port"].as<uint16_t>();
            bool compress = config["compress"].as<bool>();

            rpc::protocol<serializer> myrpc(serializer{});
            
            myrpc.set_logger(&lg);

            myrpc.register_handler(1, [x = 0](int i) mutable { fmt::print("test1 count {:d} got {:d}\n", ++x, i); });
            myrpc.register_handler(2, [](int a, int b){ fmt::print("test2 got {:d} {:d}\n", a, b); return make_ready_future<int>(a+b); });
            myrpc.register_handler(3, [](double x){ fmt::print("test3 got {:f}\n", x); return std::make_unique<double>(sin(x)); });
            myrpc.register_handler(4, [](){ fmt::print("test4 throw!\n"); throw std::runtime_error("exception!"); });
            myrpc.register_handler(5, [](){ fmt::print("test5 no wait\n"); return rpc::no_wait; });
            myrpc.register_handler(6, [](const rpc::client_info& info, int x){ fmt::print("test6 client {}, {:d}\n", inet_ntoa(info.addr.as_posix_sockaddr_in().sin_addr), x); });
            myrpc.register_handler(7, [](long a, long b) mutable {
                auto p = make_lw_shared<promise<>>();
                auto t = make_lw_shared<timer<>>();
                fmt::print("test7 got {:d} {:d}\n", a, b);
                auto f = p->get_future().then([a, b, t] {
                    fmt::print("test7 calc res\n");
                    return a - b;
                });
                t->set_callback([p = std::move(p)] () mutable { p->set_value(); });
                t->arm(1s);
                return f;
            });
            myrpc.register_handler(8, [](){ fmt::print("test8 sleep for 2 sec\n"); return sleep(2s); });
            myrpc.register_handler(9, [] (long a, long b, rpc::optional<int> c) {
                long r = 2;
                fmt::print("test9 got {:d} {:d} ", a, b);
                if (c) {
                    fmt::print("{:d}", c.value());
                    r++;
                }
                fmt::print("\n");
                return r;
            });
            myrpc.register_handler(10, [] {
                fmt::print("test 10\n");
                return make_ready_future<rpc::tuple<long, int>>(rpc::tuple<long, int>(1, 2));
            });
            myrpc.register_handler(11, [] {
                fmt::print("test 11\n");
                return 1ul;
            });
            myrpc.register_handler(12, [] (int sleep_ms, sstring payload) {
                return sleep(std::chrono::milliseconds(sleep_ms)).then([] {
                    return make_ready_future<>();
                });
            });
            myrpc.register_handler(13, [](){ fmt::print("test13 sleep for 1 msec\n"); return sleep(1ms); });
            myrpc.register_handler(14, [](sstring payload){ fmt::print("test message to big, should not get here"); });
            myrpc.register_handler(15, [] (sstring payload) {
                fmt::print("test15 echo got {:d} bytes\n", payload.size());
                return payload;
            });

            rpc::resource_limits limits;
            limits.bloat_factor = 1;
            limits.basic_request_size = 0;
            limits.max_memory = 10'000'000;
            rpc::server_options so;
            if (compress) {
                so.compressor_factory = &mc;
            }

            fmt::print("Seastar RPC server listening on port {} ...\n", port);

            auto server = rpc::protocol<serializer>::server(myrpc, so, ipv4_addr{port}, limits);
        
            auto stop_server = seastar::defer([&server] () noexcept { server.stop().get(); });
            stop_signal.wait().get();

            return 0;
        });
    });
}
