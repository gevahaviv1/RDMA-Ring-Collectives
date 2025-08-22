#include "serverlist.h"

#include <ctype.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

int parse_server_list(const char *serverlist,
                      const char *my_hostname,
                      char ***hosts_out,
                      size_t *num_hosts_out,
                      size_t *my_index_out) {
    if (!serverlist || !my_hostname || !hosts_out || !num_hosts_out || !my_index_out) {
        return -1;
    }

    char **hosts = NULL;
    size_t count = 0;
    ssize_t my_rank = -1;

    const char *p = serverlist;
    while (*p) {
        while (isspace((unsigned char)*p)) p++;
        if (!*p) break;
        const char *start = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        size_t len = (size_t)(p - start);
        char *host = malloc(len + 1);
        if (!host) {
            free_host_list(hosts, count);
            return -1;
        }
        memcpy(host, start, len);
        host[len] = '\0';
        char **tmp = realloc(hosts, sizeof(*hosts) * (count + 1));
        if (!tmp) {
            free(host);
            free_host_list(hosts, count);
            return -1;
        }
        hosts = tmp;
        hosts[count] = host;

        size_t my_len = strlen(my_hostname);
        if (my_rank == -1 &&
            ((my_len >= len && strncmp(my_hostname, host, len) == 0) ||
             (len >= my_len && strncmp(host, my_hostname, my_len) == 0))) {
            my_rank = (ssize_t)count;
        }

        count++;
    }

    if (my_rank == -1) {
        free_host_list(hosts, count);
        return -1;
    }

    *hosts_out = hosts;
    *num_hosts_out = count;
    *my_index_out = (size_t)my_rank;
    return 0;
}

void free_host_list(char **hosts, size_t count) {
    if (!hosts) return;
    for (size_t i = 0; i < count; ++i) {
        free(hosts[i]);
    }
    free(hosts);
}
