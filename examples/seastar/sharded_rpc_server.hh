#pragma once

#include <memory>
#include <utility>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/rpc/rpc.hh>

using namespace seastar;

// Meant to be driven through seastar::sharded<sharded_rpc_server<Serializer>>,
// one instance per shard. Seastar's accept-proxy (posix_ap_server_socket_impl)
// forwards each new TCP connection to whichever shard its load balancer
// picks; a shard that never constructed a server_socket for the address
// has nothing registered in its (thread_local) ports/sockets tables and
// silently closes the forwarded connection. Running this on every shard
// makes every shard a valid proxy target, which removes the drop.
//
// Only shard 0 (smp::main_thread()) ever performs the real bind()+listen();
// every other shard gets the lightweight proxy registration automatically
// (see posix-stack.cc: register_posix_stack), so there's no EADDRINUSE from
// calling listen() on all of them.
//
// rpc::protocol is move-only, not copyable: its handler table stores a
// seastar::gate per registered verb, and gate's copy constructor is deleted.
// So each shard must build its own protocol in place via register_handler()
// (see protocol() below) - it can't be constructed once and handed out.
template <typename Serializer>
class sharded_rpc_server {
    rpc::protocol<Serializer> _proto;
    std::unique_ptr<typename rpc::protocol<Serializer>::server> _server;

public:
    explicit sharded_rpc_server(Serializer serializer) : _proto(std::move(serializer)) {}

    // Register handlers on every shard (via invoke_on_all) before calling
    // listen() - the verb table is per-instance, not shared across shards.
    rpc::protocol<Serializer>& protocol() { return _proto; }

    void listen(uint16_t port, rpc::server_options so, rpc::resource_limits limits) {
        _server = std::make_unique<typename rpc::protocol<Serializer>::server>(
            _proto, so, seastar::ipv4_addr{port}, limits);
    }

    // seastar::sharded<T>::stop() detects and calls this automatically.
    seastar::future<> stop() {
        return _server ? _server->stop() : seastar::make_ready_future<>();
    }
};
