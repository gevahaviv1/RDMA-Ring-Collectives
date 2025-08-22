#include "bootstrap.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

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
        char ch;
        if (read(fd_left, &ch, 1) != 1) _exit(1);
        assert(ch == 'a');
        if (write(fd_right, "b", 1) != 1) _exit(1);
        close(fd_left);
        close(fd_right);
        _exit(0);
    } else {
        /* parent: rank 0 */
        if (bootstrap_ring(hosts, 2, 0, base_port, 5000, &fd_left, &fd_right) != 0)
            return 1;
        if (write(fd_right, "a", 1) != 1) return 1;
        char ch;
        if (read(fd_left, &ch, 1) != 1) return 1;
        assert(ch == 'b');
        close(fd_left);
        close(fd_right);
        int status = 0;
        waitpid(pid, &status, 0);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        printf("Bootstrap test passed\n");
    }
    return 0;
}
