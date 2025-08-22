#ifndef SERVERLIST_H
#define SERVERLIST_H

#include <stddef.h>

/*
 * Parse a whitespace-separated list of hostnames and determine the caller's rank.
 *
 * serverlist:    input string containing hostnames separated by any whitespace.
 * my_hostname:   the hostname of the current process.
 * hosts_out:     on success, will point to an array of strings containing the
 *                hostnames. The caller must free this array with free_host_list().
 * num_hosts_out: on success, number of hosts in the list.
 * my_index_out:  on success, the index of my_hostname within the list.
 *
 * Returns 0 on success, -1 if my_hostname is not found or on allocation error.
 */
int parse_server_list(const char *serverlist,
                      const char *my_hostname,
                      char ***hosts_out,
                      size_t *num_hosts_out,
                      size_t *my_index_out);

/* Free the array returned by parse_server_list. */
void free_host_list(char **hosts, size_t count);

#endif /* SERVERLIST_H */
