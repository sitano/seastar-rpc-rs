#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include "rpc_serializer.hh"

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

namespace {

void print_usage(const char* prog) {
    fmt::print(
        "usage: {} --server host:port [--compress bool] [--timeout ms] VERB [args...]\n"
        "\n"
        "verbs:\n"
        "   1  <int>                     fire, no reply value\n"
        "   2  <int> <int>               returns a+b\n"
        "   3  <double>                  returns sin(x)\n"
        "   4                            always throws server-side\n"
        "   5                            no_wait (fire and forget)\n"
        "   6  <int>                     server echoes client_info + x\n"
        "   7  <long> <long>             returns a-b (server delays 1s)\n"
        "   8                            server sleeps 2s\n"
        "   9  <long> <long> [int]       3rd arg optional\n"
        "  10                            returns tuple<long,int>\n"
        "  11                            returns uint64\n"
        "  12 <int sleep_ms> <string>    void\n"
        "  13                            cancellable sleep(1ms)\n"
        "  14 <string>                   void, capped by server's size limit\n"
        "  15 <string>                   echoes payload back\n",
        prog);
}

future<> dispatch_verb(
        rpc::protocol<serializer>& myrpc, rpc::protocol<serializer>::client& client,
        int verb, const std::vector<std::string>& args, std::chrono::milliseconds timeout);

}

static mycomp mc;
static logger lg("rpc_demo");

int main(int ac, char** av) {
    app_template app;
    app.add_options()
                    ("server", bpo::value<std::string>()->default_value("127.0.0.1:10000"), "Server address host:port")
                    ("compress", bpo::value<bool>()->default_value(false), "Compress RPC traffic")
                    ("timeout", bpo::value<uint32_t>()->default_value(5000), "Call timeout in ms");
    app.add_positional_options({
        { "verb", bpo::value<int>(), "Verb ID to call (1-15)", 1 },
        { "args", bpo::value<std::vector<std::string>>(), "Verb-specific arguments", -1 },
    });

    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        if (!config.count("server") || !config.count("verb")) {
            print_usage(av[0]);
            return make_ready_future<>().then([] { engine().exit(1); });
        }

        bool compress = config["compress"].as<bool>();
        auto timeout = std::chrono::milliseconds(config["timeout"].as<uint32_t>());
        int verb = config["verb"].as<int>();
        std::vector<std::string> args = config.count("args")
                ? config["args"].as<std::vector<std::string>>()
                : std::vector<std::string>{};

        rpc::client_options co;
        if (compress) {
            co.compressor_factory = &mc;
        }

        auto myrpc = std::make_unique<rpc::protocol<serializer>>(serializer{});
        myrpc->set_logger(&lg);

        auto client = std::make_unique<rpc::protocol<serializer>::client>(*myrpc, co, ipv4_addr{config["server"].as<std::string>()});

        future<> call = make_ready_future<>();
        try {
            call = dispatch_verb(*myrpc, *client, verb, args, timeout);
        } catch (std::exception& e) {
            fmt::print("{}\n", e.what());
            print_usage(av[0]);
            return client->stop().then([client = std::move(client), rpc = std::move(myrpc)] {
                engine().exit(1);
            });
        }

        return call.then_wrapped([] (future<> f) {
            try {
                f.get();
            } catch (rpc::timeout_error&) {
                fmt::print("call timed out\n");
            } catch (std::exception& e) {
                fmt::print("call failed: {}\n", e.what());
            }
        }).finally([client = std::move(client), rpc = std::move(myrpc)] () mutable {
            return client->stop().then([client = std::move(client), rpc = std::move(rpc)] { 
                engine().exit(0);
            });
        });
    });
}

namespace {

template <typename T>
T parse(const std::string& s);

template <> int parse<int>(const std::string& s) { return std::stoi(s); }
template <> long parse<long>(const std::string& s) { return std::stol(s); }
template <> double parse<double>(const std::string& s) { return std::stod(s); }
template <> sstring parse<sstring>(const std::string& s) { return sstring(s); }

future<> dispatch_verb(
        rpc::protocol<serializer>& myrpc, rpc::protocol<serializer>::client& client,
        int verb, const std::vector<std::string>& args, std::chrono::milliseconds timeout) {

    auto need = [&] (size_t n) {
        if (args.size() < n) {
            throw std::runtime_error(fmt::format("verb {:d} needs at least {:d} argument(s), got {:d}", verb, n, args.size()));
        }
    };

    switch (verb) {
    case 1: {
        need(1);
        auto stub = myrpc.make_client<void (int)>(1);
        return stub(client, timeout, parse<int>(args[0]));
    }
    case 2: {
        need(2);
        auto stub = myrpc.make_client<int (int, int)>(2);
        return stub(client, timeout, parse<int>(args[0]), parse<int>(args[1])).then([] (int r) {
            fmt::print("verb 2 -> {:d}\n", r);
        });
    }
    case 3: {
        need(1);
        auto stub = myrpc.make_client<double (double)>(3);
        return stub(client, timeout, parse<double>(args[0])).then([] (double r) {
            fmt::print("verb 3 -> {:f}\n", r);
        });
    }
    case 4: {
        auto stub = myrpc.make_client<void ()>(4);
        return stub(client, timeout);
    }
    case 5: {
        auto stub = myrpc.make_client<rpc::no_wait_type ()>(5);
        return stub(client, timeout);
    }
    case 6: {
        need(1);
        auto stub = myrpc.make_client<void (int)>(6);
        return stub(client, timeout, parse<int>(args[0]));
    }
    case 7: {
        need(2);
        auto stub = myrpc.make_client<long (long, long)>(7);
        return stub(client, timeout, parse<long>(args[0]), parse<long>(args[1])).then([] (long r) {
            fmt::print("verb 7 -> {:d}\n", r);
        });
    }
    case 8: {
        auto stub = myrpc.make_client<void ()>(8);
        return stub(client, timeout);
    }
    case 9: {
        need(2);
        if (args.size() > 2) {
            auto stub = myrpc.make_client<long (long, long, int)>(9);
            return stub(client, timeout, parse<long>(args[0]), parse<long>(args[1]), parse<int>(args[2])).then([] (long r) {
                fmt::print("verb 9 -> {:d}\n", r);
            });
        } else {
            auto stub = myrpc.make_client<long (long, long)>(9);
            return stub(client, timeout, parse<long>(args[0]), parse<long>(args[1])).then([] (long r) {
                fmt::print("verb 9 -> {:d}\n", r);
            });
        }
    }
    case 10: {
        auto stub = myrpc.make_client<future<rpc::tuple<long, int>> ()>(10);
        return stub(client, timeout).then([] (rpc::tuple<long, int> r) {
            fmt::print("verb 10 -> {:d}, {:d}\n", std::get<0>(r), std::get<1>(r));
        });
    }
    case 11: {
        auto stub = myrpc.make_client<uint64_t ()>(11);
        return stub(client, timeout).then([] (uint64_t r) {
            fmt::print("verb 11 -> {:d}\n", r);
        });
    }
    case 12: {
        need(2);
        auto stub = myrpc.make_client<void (int, sstring)>(12);
        return stub(client, timeout, parse<int>(args[0]), parse<sstring>(args[1]));
    }
    case 13: {
        auto stub = myrpc.make_client<void ()>(13);
        return stub(client, timeout);
    }
    case 14: {
        need(1);
        auto stub = myrpc.make_client<void (sstring)>(14);
        return stub(client, timeout, parse<sstring>(args[0]));
    }
    case 15: {
        need(1);
        auto stub = myrpc.make_client<sstring (sstring)>(15);
        return stub(client, timeout, parse<sstring>(args[0])).then([] (sstring r) {
            fmt::print("verb 15 -> {}\n", r);
        });
    }
    default:
        throw std::runtime_error(fmt::format("unknown verb id: {:d}", verb));
    }
}

}