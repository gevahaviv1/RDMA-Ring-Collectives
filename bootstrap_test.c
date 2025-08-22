#include "bootstrap.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

int main(void) {
    const char *hosts[2] = {"127.0.0.1", "127.0.0.1"};
    const int base_port = 40000;
    int fd_left = -1, fd_right = -1;
    pid_t pid = fork();
    if (pid < 0) return 1;

    if (pid == 0) {
        /* child: rank 1 */
        if (bootstrap_ring(hosts, 2, 1, base_port, 5000, &fd_left, &fd_right) != 0)
            _exit(1);

        /* simple byte exchange */
        char ch;
        if (read(fd_left, &ch, 1) != 1) _exit(1);
        assert(ch == 'a');
        if (write(fd_right, "b", 1) != 1) _exit(1);

        /* QP bootstrap exchange */
        struct qp_boot my = {
            .qpn = 11,
            .psn = 21,
            .lid = 31,
            .gid = {0},
            .base_addr = 41,
            .rkey = 51,
            .bytes = 61
        };
        memset(my.gid, 1, sizeof(my.gid));
        struct qp_boot left, right;
        if (exchange_qp_boot(fd_left, fd_right, &my, &left, &right, 5000) != 0)
            _exit(1);
        assert(left.qpn == 10 && right.qpn == 10);

        close(fd_left);
        close(fd_right);
        _exit(0);
    } else {
        /* parent: rank 0 */
        if (bootstrap_ring(hosts, 2, 0, base_port, 5000, &fd_left, &fd_right) != 0)
            return 1;

        /* simple byte exchange */
        if (write(fd_right, "a", 1) != 1) return 1;
        char ch;
        if (read(fd_left, &ch, 1) != 1) return 1;
        assert(ch == 'b');

        /* QP bootstrap exchange */
        struct qp_boot my = {
            .qpn = 10,
            .psn = 20,
            .lid = 30,
            .gid = {0},
            .base_addr = 40,
            .rkey = 50,
            .bytes = 60
        };
        memset(my.gid, 0, sizeof(my.gid));
        struct qp_boot left, right;
        if (exchange_qp_boot(fd_left, fd_right, &my, &left, &right, 5000) != 0)
            return 1;
        assert(left.qpn == 11 && right.qpn == 11);

        close(fd_left);
        close(fd_right);
        int status = 0;
        waitpid(pid, &status, 0);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        printf("Bootstrap test passed\n");
    }
    return 0;
}
