#ifndef CEPH_UCXEVENT_H
#define CEPH_UCXEVENT_H

#include <vector>
#include <queue>

#include "msg/async/Stack.h"
#include "msg/async/Event.h"
#include "msg/async/EventEpoll.h"

#include "common/Mutex.h"

extern "C" {
#include <ucp/api/ucp.h>
};

struct ucx_rx_buf {
    size_t length;
    size_t offset;

    uint8_t *rdata;
};

struct ucx_req_descr {
    bufferlist *bl;
    ucp_dt_iov_t *iov_list;
    ucx_rx_buf *rx_buf;
    void *rx_queue;
};

struct ucx_connect_message {
    uint16_t addr_len;
    uint32_t remote_fd;
} __attribute__ ((packed));

typedef struct {
    ucp_ep_h  ucp_ep;
    ucs_status_ptr_t close_request;
    std::deque<bufferlist *> pending;
    std::deque<ucx_rx_buf *> rx_queue;
} connection_t;

class UCXDriver : public EpollDriver {
    private:
        CephContext *cct;

        int ucp_fd = -1;
        ucp_worker_h ucp_worker;

        Mutex lock; /* Protects 'connecting' pool */

        std::set<int> connecting; /* TODO: remove */
        /* server fd + ep address */
        std::list<std::pair<int, ucp_ep_address_t *> > conn_queue;
        std::set<int> waiting_events;

        std::set<int> undelivered;
        std::map<int, connection_t> connections;

        void event_progress(vector<FiredFileEvent> &fired_events);
        void dispatch_events(vector<FiredFileEvent> &fired_events);

        void insert_rx(int fd, uint8_t *rdata, size_t length);

        bool in_set(std::set<int> set, int fd) {
            return set.find(fd) != set.end();
        }

        char *recv_addr(int fd);
        int send_addr(int fd,
                      ucp_address_t *ucp_addr,
                      size_t ucp_addr_len);

        int conn_create(int fd, ucp_ep_h ep);
        int recv_stream(int fd);

        void conn_release_recvs(int fd);
        void ucx_ep_close(int fd, bool close_event);

    public:
        UCXDriver(CephContext *c): EpollDriver(c), cct(c),
                                   lock("UCXDriver::lock") {}
        virtual ~UCXDriver();

        int init(EventCenter *c, int nevent) override;
        int add_event(int fd, int cur_mask, int add_mask) override;
        int del_event(int fd, int cur_mask, int del_mask) override;
        int resize_events(int newsize) override;
        int event_wait(vector<FiredFileEvent> &fired_events, struct timeval *tp) override;

        void cleanup(ucp_address_t *ucp_addr);
        void addr_create(ucp_context_h ucp_context,
                         ucp_address_t **ucp_addr,
                         size_t *ucp_addr_len);

        int conn_establish(int fd, ucp_ep_h ep);

        void conn_enqueue(int fd, ucp_ep_address_t *ep_addr);
        bool has_conn_reqs() const {
            return !conn_queue.empty();
        }
        std::pair<int, ucp_ep_address_t*> conn_dequeue(int fd);
        int get_first_conn_fd() const {
//            ucs_assert(has_conn_reqs());
            return conn_queue.front().first;
        }

        void conn_close(int fd);
        void conn_shutdown(int fd);

        int is_connected(int fd) {
            return (connections.count(fd) > 0 &&
                    NULL != connections[fd].ucp_ep);
        }

        ssize_t send(int fd, bufferlist &bl, bool more);

        static void send_completion_cb(void *request, ucs_status_t status);
        static void send_completion(ucx_req_descr *descr) {
            descr->bl->clear();
            if (descr->iov_list) {
                delete descr->iov_list;
            }
        }

        int read(int fd, char *rbuf, size_t bytes);
        ucp_worker_h get_ucp_worker() const {
            return ucp_worker;
        }
        int  get_ucp_fd() const {
            return ucp_fd;
        }
};

#endif //CEPH_UCXEVENT_H
