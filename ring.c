#include "ring.h"

int left_of(int rank, int n) {
    if (n <= 0) return -1;
    int r = rank % n;
    if (r < 0) r += n;
    return (r + n - 1) % n;
}

int right_of(int rank, int n) {
    if (n <= 0) return -1;
    int r = rank % n;
    if (r < 0) r += n;
    return (r + 1) % n;
}
