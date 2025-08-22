#ifndef RING_H
#define RING_H

/* Compute the index of the neighbor to the left in a ring of size n.
 * Returns -1 if n <= 0.
 */
int left_of(int rank, int n);

/* Compute the index of the neighbor to the right in a ring of size n.
 * Returns -1 if n <= 0.
 */
int right_of(int rank, int n);

#endif /* RING_H */
