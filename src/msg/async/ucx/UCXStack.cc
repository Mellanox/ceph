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

#include "UCXStack.h"
#include "UCXEvent.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "UCXStack "

#include <mutex>
#include <map>
#include <arpa/inet.h>

UCXGlobals UCXGl;


UCXConnectedSocketImpl::UCXConnectedSocketImpl(UCXWorker *w,
                                               ucp_ep_address_t *ep_addr)
    : worker(w), ucp_ep_addr(ep_addr), ucp_ep(NULL), my_fd(0)
{
}

void UCXConnectedSocketImpl::shutdown()
{
//    ldout(cct(), 20) << __func__ << " conn fd: "
//                     << tcp_fd << " is shutting down..." << dendl;

    /* TODO: ucp_ep_close_nb(SYNC) ? */
    /* Call for shutdown even ucp_ep doesn't exist yet */
    if (my_fd > 0) {
        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
//       //::shutdown(tcp_fd, SHUT_RDWR); //Vasily: ????
        driver->conn_shutdown(my_fd);
    }
}

void UCXConnectedSocketImpl::close()
{
    UCXConnectedSocketImpl::shutdown();

//    if (tcp_fd > 0) {
//        ldout(cct(), 20) << __func__ << " fd: " << tcp_fd << dendl;
//
//        UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
//        driver->conn_close(tcp_fd);
//
//        ::close(tcp_fd);
//        tcp_fd = -1;
//    }
}

UCXConnectedSocketImpl::~UCXConnectedSocketImpl()
{
    UCXConnectedSocketImpl::close();
}

int UCXConnectedSocketImpl::is_connected()
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->is_connected(my_fd);
}

int UCXConnectedSocketImpl::connect(const entity_addr_t& peer_addr, const SocketOptions &opts)
{
//    NetHandler net(cct());
//    int ret;
//
//    tcp_fd = net.connect(peer_addr, opts.connect_bind_addr);
//    if (tcp_fd < 0) {
//        lderr(cct()) << __func__ << " failed to allocate client socket" << dendl;
//        return -errno;
//    }
//
//
//    if (ret < 0) {
//        return ret;
//    }
    assert(ucp_ep == NULL);
    assert(ucp_ep_addr == NULL);
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_FLAGS |
                           UCP_EP_PARAM_FIELD_SOCK_ADDR |
                           UCP_EP_PARAM_FIELD_USER_DATA;
    ep_params.user_data        = this;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    struct sockaddr_in addr;
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = inet_addr("1.1.75.1");
    addr.sin_port        = peer_addr.get_port();
    ep_params.sockaddr.addr    = (struct sockaddr*)&addr;//.sin_addr; peer_addr.get_sockaddr();
    ep_params.sockaddr.addrlen = sizeof(struct sockaddr);
//    ep_params.sockaddr.addr    = peer_addr.get_sockaddr();
//    ep_params.sockaddr.addrlen = peer_addr.get_sockaddr_len();
    ucs_status_t status = ucp_ep_create(worker->get_ucp_worker(), &ep_params, &ucp_ep);
    ldout(cct(), 0) << __func__ << " creating UCP ep to addr: "
                    << inet_ntoa(addr.sin_addr) << ":"
                    << addr.sin_port << " status: "
                    << ucs_status_string(status) << dendl;
    if (status == UCS_OK) {
        my_fd = UCXGl.get_fd(ucp_ep, worker->get_ucp_fd());
        return worker->conn_establish(my_fd, ucp_ep);
    }
    return status;
}

int UCXConnectedSocketImpl::accept(int server_sock,
                                   entity_addr_t *out,
                                   const SocketOptions &opt)
{
    ucp_ep_params_t ep_params;
    ep_params.field_mask  = UCP_EP_PARAM_FIELD_EP_ADDR;
    ep_params.ep_addr     = ucp_ep_addr;
    assert(ucp_ep == NULL);
    ucs_status_t status = ucp_ep_create(worker->get_ucp_worker(), &ep_params,
                                        &ucp_ep);
    if (status == UCS_OK) {
        my_fd = UCXGl.get_fd(ucp_ep, worker->get_ucp_fd());
        return worker->conn_establish(my_fd, ucp_ep);
    }
    return status;
}

int UCXWorker::conn_establish(int fd, ucp_ep_h ep)
{
    return driver->conn_establish(fd, ep);
}

ssize_t UCXConnectedSocketImpl::read(int fd_or_id, char *buf, size_t len)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->read(my_fd, buf, len);
}

ssize_t UCXConnectedSocketImpl::zero_copy_read(bufferptr&)
{
    lderr(cct()) << __func__ << dendl;
    return 0;
}

void UCXConnectedSocketImpl::request_init(void *req)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    desc->rx_buf = NULL;
    desc->bl = new bufferlist;
}

void UCXConnectedSocketImpl::request_cleanup(void *req)
{
    ucx_req_descr *desc = static_cast<ucx_req_descr *>(req);

    delete desc->bl;
}

ssize_t UCXConnectedSocketImpl::send(bufferlist &bl, bool more)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    return driver->send(my_fd, bl, more);
}

UCXServerSocketImpl::UCXServerSocketImpl(UCXWorker *w)
    : worker(w), my_fd(UCXGl.get_fd(this))
{
}

UCXServerSocketImpl::~UCXServerSocketImpl()
{
    UCXGl.release_ss(this);
    if (ucp_listener) {
        ucp_listener_destroy(ucp_listener);
    }
}

int UCXServerSocketImpl::listen(entity_addr_t &sa, const SocketOptions &opt)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    assert(AF_INET == sa.get_family());
    addr.sin_family      = AF_INET; // sa.get_family();       /* ? , */
    addr.sin_addr.s_addr = inet_addr("1.1.75.1");// INADDR_ANY; //sai_p->sin_addr.s_addr;    /* ? INADDR_ANY, */
//    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = sa.get_port();

    ucp_listener_params_t params;
    params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                                UCP_LISTENER_PARAM_FIELD_ACCEPT_HANDLER2;
    params.sockaddr.addr      = reinterpret_cast<const struct sockaddr*>(&addr);
    params.sockaddr.addrlen   = sizeof(addr);
    params.accept_handler2.cb  = server_accept_cb;
    params.accept_handler2.arg = this;

    /* Create a listener on the server side to listen on the given address.*/
    ucs_status_t status = ucp_listener_create(worker->get_ucp_worker(),
                                              &params, &ucp_listener);
    ldout(cct(), 0) << __func__ << " creating UCP listener to addr: "
                    << inet_ntoa(addr.sin_addr) << ":"
                    << addr.sin_port << " status: "
                    << ucs_status_string(status) << dendl;

    if (status == UCS_OK) {
        return 0;
    }

    fprintf(stderr, "failed to listen (%s)\n", ucs_status_string(status));
    ucp_listener = NULL;
    return status;
}

int UCXServerSocketImpl::accept(ConnectedSocket *sock, const SocketOptions &opt,
                                entity_addr_t *out, Worker *w)
{
    UCXDriver *driver = dynamic_cast<UCXDriver *>(worker->center.get_driver());
    if (!driver->has_conn_reqs()) {
        return -EAGAIN;
    }

    UCXConnectedSocketImpl *p =
        new UCXConnectedSocketImpl(dynamic_cast<UCXWorker *>(w),
                                   driver->conn_dequeue(my_fd).second);

    int r = p->accept(-1, out, opt);
    if (r < 0) {
        ldout(cct(), 1) << __func__ << " accept failed on server socket: "
                        << worker->get_ucp_fd() << dendl;
        delete p;

        return r;
    }

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    return 0;
}

void UCXServerSocketImpl::abort_accept()
{
    assert(0);
    if (ucp_listener) {
        /* TODO: ???*/;
        ucp_listener_destroy(ucp_listener);
    }

    ucp_listener = NULL;
}

UCXWorker::UCXWorker(CephContext *c, unsigned i) : Worker(c, i)
{
}

UCXWorker::~UCXWorker()
{
}

int UCXWorker::listen(entity_addr_t &addr, const SocketOptions &opts, ServerSocket *sock)
{
    UCXServerSocketImpl *p = new UCXServerSocketImpl(this);

    int r = p->listen(addr, opts);
    if (r < 0) {
        delete p;
        return r;
    }

    *sock = ServerSocket(std::unique_ptr<ServerSocketImpl>(p));
    return 0;
}

int UCXWorker::connect(const entity_addr_t &addr, const SocketOptions &opts, ConnectedSocket *sock)
{
    UCXConnectedSocketImpl *p = new UCXConnectedSocketImpl(this);

    int r = p->connect(addr, opts);
    if (r < 0) {
        ldout(cct, 1) << __func__ << " try connecting failed." << dendl;
        delete p;

        return r;
    }

    std::unique_ptr<UCXConnectedSocketImpl> csi(p);
    *sock = ConnectedSocket(std::move(csi));

    return 0;
}

void UCXWorker::initialize()
{
    driver = dynamic_cast<UCXDriver *>(center.get_driver());
    driver->addr_create(get_stack()->get_ucp_context(),
                        &ucp_addr, &ucp_addr_len);
}

void UCXWorker::destroy()
{
    driver->cleanup(ucp_addr);
}

void UCXWorker::set_stack(UCXStack *s)
{
    stack = s;
}

UCXStack::UCXStack(CephContext *cct, const string &t) : NetworkStack(cct, t)
{

    ucs_status_t status;
    ucp_config_t *ucp_config;
    ucp_params_t params;

    ldout(cct, 10) << __func__ << " constructing UCX stack " << t
                   << " with " << get_num_worker() << " workers " << dendl;

    int rc = setenv("UCX_CEPH_NET_DEVICES", cct->_conf->ms_async_ucx_device.c_str(), 1);
    if (rc) {
        lderr(cct) << __func__ << " failed to export UCX_CEPH_NET_DEVICES. Application aborts." << dendl;
        ceph_abort();
    }

    rc = setenv("UCX_CEPH_TLS", cct->_conf->ms_async_ucx_tls.c_str(), 1);
    if (rc) {
        lderr(cct) << __func__ << " failed to export UCX_CEPH_TLS. Application aborts." << dendl;
        ceph_abort();
    }

    status = ucp_config_read("CEPH", NULL, &ucp_config);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << "failed to read UCP config" << dendl;
        ceph_abort();
    }

    memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES        |
                        UCP_PARAM_FIELD_REQUEST_SIZE    |
                        UCP_PARAM_FIELD_REQUEST_INIT    |
                        UCP_PARAM_FIELD_REQUEST_CLEANUP |
                        UCP_PARAM_FIELD_TAG_SENDER_MASK |
                        UCP_PARAM_FIELD_MT_WORKERS_SHARED;

    params.features   = UCP_FEATURE_WAKEUP |
                        UCP_FEATURE_STREAM;

    params.mt_workers_shared = 1;
    params.tag_sender_mask = -1;
    params.request_size    = sizeof(ucx_req_descr);
    params.request_init    = UCXConnectedSocketImpl::request_init;
    params.request_cleanup = UCXConnectedSocketImpl::request_cleanup;

    status = ucp_init(&params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);
    if (UCS_OK != status) {
        lderr(cct) << __func__ << "failed to init UCP context" << dendl;
        ceph_abort();
    }
    ucp_context_print_info(ucp_context, stdout);

    for (unsigned i = 0; i < get_num_worker(); i++) {
        UCXWorker *w = dynamic_cast<UCXWorker *>(get_worker(i));
        w->set_stack(this);
    }
}

UCXStack::~UCXStack()
{
    ucp_cleanup(ucp_context);
}

void UCXStack::spawn_worker(unsigned i, std::function<void ()> &&func)
{
    threads.resize(i+1);
    threads[i] = std::thread(func);
}

void UCXStack::join_worker(unsigned i)
{
    assert(threads.size() > i && threads[i].joinable());
    threads[i].join();
}
