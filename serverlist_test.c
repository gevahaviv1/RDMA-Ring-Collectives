#include "serverlist.h"

#include <assert.h>
#include <stdio.h>

static void test_single_host(void) {
    const char *servers = "host1";
    char **hosts = NULL;
    size_t count = 0, my_index = 0;
    int rc = parse_server_list(servers, "host1", &hosts, &count, &my_index);
    assert(rc == 0);
    assert(count == 1);
    assert(my_index == 0);
    free_host_list(hosts, count);
}

static void test_four_hosts(void) {
    const char *servers = "host1 host2\n   host3\thost4";
    char **hosts = NULL;
    size_t count = 0, my_index = 0;
    int rc = parse_server_list(servers, "host3.example.com", &hosts, &count, &my_index);
    assert(rc == 0);
    assert(count == 4);
    assert(my_index == 2);
    free_host_list(hosts, count);
}

static void test_host_not_found(void) {
    const char *servers = "host1 host2";
    char **hosts = NULL;
    size_t count = 0, my_index = 0;
    int rc = parse_server_list(servers, "host3", &hosts, &count, &my_index);
    assert(rc == -1);
    assert(hosts == NULL);
    assert(count == 0);
    assert(my_index == 0);
}

int main(void) {
    test_single_host();
    test_four_hosts();
    test_host_not_found();
    printf("All tests passed\n");
    return 0;
}
