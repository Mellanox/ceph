// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017  Mellanox Technologies Ltd. All rights reserved.
 *
 *
 * Author: Alex Mikheev <alexm@mellanox.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_MSG_UCXSTACK_H
#define CEPH_MSG_UCXSTACK_H

#include <vector>
#include <thread>
#include <deque>
#include <queue>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"

#include "msg/async/Stack.h"
#include "UCXEvent.h"

extern "C" {
#include <ucp/api/ucp.h>
};

class UCXStack;
class UCXConnectedSocketImpl;

class UCXWorker : public Worker {
    UCXStack *stack;
    ucp_address_t *ucp_addr;
    size_t ucp_addr_len;

    UCXDriver *driver;
    // pass received messages to socket(s)
    void dispatch_rx();

public:
    explicit UCXWorker(CephContext *c, unsigned i);
    virtual ~UCXWorker();

    virtual int listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *) override;
    virtual int connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *socket) override;

    virtual void initialize() override;
    virtual void destroy() override;

    int conn_establish(int fd, ucp_ep_h ep);

    void set_stack(UCXStack *s);
    UCXStack *get_stack() { return stack; }
    ucp_worker_h get_ucp_worker() const {
        return driver->get_ucp_worker();
    }
    int get_ucp_fd() const {
        return driver->get_ucp_fd();
    }
};

class UCXConnectedSocketImpl : public ConnectedSocketImpl {
private:
    UCXWorker *worker;
    ucp_ep_address_t *ucp_ep_addr;
    ucp_ep_h          ucp_ep;
//    static int _global_fd_counter = 0;
    int my_fd;
    CephContext *cct() { return worker->cct; }

public:
    UCXConnectedSocketImpl(UCXWorker *w, ucp_ep_address_t *ep_addr = NULL);
    virtual ~UCXConnectedSocketImpl();

    int connect(const entity_addr_t& peer_addr, const SocketOptions &opt);
    int accept(int server_sock, entity_addr_t *out, const SocketOptions &opt);

    // interface functions
    virtual int is_connected() override;
    virtual ssize_t read(int, char*, size_t) override;
    virtual ssize_t zero_copy_read(bufferptr&) override;
    virtual ssize_t send(bufferlist &bl, bool more) override;
    virtual void shutdown() override;
    virtual void close() override;
    virtual int fd() const override { return my_fd; }

    //ucp request magic
    static void request_init(void *req);
    static void request_cleanup(void *req);
};

class UCXServerSocketImpl : public ServerSocketImpl {
private:
    UCXWorker *worker;
    ucp_listener_h ucp_listener = NULL;
    int my_fd;

    CephContext *cct() { return worker->cct; }
    static void server_accept_cb(ucp_ep_address_t *ep_addr, void *self) {
        UCXServerSocketImpl *server =
                                reinterpret_cast<UCXServerSocketImpl *>(self);
        UCXDriver *driver = dynamic_cast<UCXDriver *>(server->worker->center.get_driver());
        driver->conn_enqueue(server->my_fd, ep_addr);
    }
public:
    UCXServerSocketImpl(UCXWorker *w);
    ~UCXServerSocketImpl();

    int listen(entity_addr_t &sa, const SocketOptions &opt);

    // interface functions
    virtual int accept(ConnectedSocket *sock, const SocketOptions &opt,
                       entity_addr_t *out, Worker *w) override;
    virtual void abort_accept() override;
    // Get file descriptor
    virtual int fd() const override {
        return my_fd;
    }
};

class UCXStack : public NetworkStack {
    vector<std::thread> threads;
    ucp_context_h ucp_context;
public:
    explicit UCXStack(CephContext *cct, const string &t);
    virtual ~UCXStack();
    virtual bool support_zero_copy_read() const override { return false; }
    virtual bool nonblock_connect_need_writable_event() const { return false; }

    virtual void spawn_worker(unsigned i, std::function<void ()> &&func) override;
    virtual void join_worker(unsigned i) override;

    ucp_context_h get_ucp_context() { return ucp_context; }
};

class UCXGlobals {
    static const int        fd_max         = std::numeric_limits<int16_t>::max() * 2;
    int                     fd_cntr        = std::numeric_limits<int16_t>::max();
    int                     fd_server_cntr = fd_max;

    mutable std::mutex      lock;

    std::map<int, ucp_ep_h>             fd2ep_map;
    std::map<int, int>                  fd2ucxfd_map;
    std::map<int, UCXServerSocketImpl*> fd2ss;
    std::map<UCXServerSocketImpl*, int> ss2fd;
public:
    static bool is_server_fd(int fd) {
        return fd > fd_max;
    }

    UCXServerSocketImpl *get_ss(int fd) const {
        UCXServerSocketImpl *ss = NULL;
        lock.lock();
        std::map<int, UCXServerSocketImpl*>::const_iterator i = fd2ss.find(fd);
        if (i != fd2ss.end()) {
            ss = i->second;
        }
        lock.unlock();
        return ss;
    }

    int get_ucx_fd(int fd) const {
        int ucx_fd = 0;
        lock.lock();
        std::map<int, int>::const_iterator i = fd2ucxfd_map.find(fd);
        if (i != fd2ucxfd_map.end()) {
            ucx_fd = i->second;
        }
        lock.unlock();
        return ucx_fd;
    }

    int get_fd(const ucp_ep_h ep, int ucp_fd) {
        lock.lock();
        int fd = ++fd_cntr;
        assert(fd_cntr < fd_max);
        assert(fd2ep_map.count(fd) == 0);
        fd2ep_map[fd]    = ep;
        fd2ucxfd_map[fd] = ucp_fd;
        lock.unlock();
        return fd;
    }

    int get_fd(UCXServerSocketImpl *ss) {
        lock.lock();
        int fd = ++fd_server_cntr;
        assert(fd_server_cntr > fd_max);
        assert(fd2ss.count(fd) == 0);
        fd2ss[fd] = ss;
        ss2fd[ss] = fd;
        lock.unlock();
        return fd;
    }

    void release_fd(int fd) {
        lock.lock();
        if (fd > fd_max) {
            ss2fd.erase(fd2ss[fd]);
            fd2ss.erase(fd);
        } else {
            assert(fd2ep_map.count(fd) > 0);
            fd2ep_map.erase(fd);
            fd2ucxfd_map.erase(fd);
        }
        lock.unlock();
    }

    void release_ss(UCXServerSocketImpl *ss) {
        fd2ss.erase(ss2fd[ss]);
        ss2fd.erase(ss);
    }

};


#endif
