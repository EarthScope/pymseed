#ifndef PYMSEED_SCAN_H
#define PYMSEED_SCAN_H

#include <stdint.h>

/* pm_scan_records() stopped at the end of the buffer, or at a record that
   continues past it */
#define PM_SCAN_NEEDMORE 0
/* pm_scan_records() stopped because the batch is full */
#define PM_SCAN_FULL 1
/* pm_scan_records() stopped before input it will skip (MSF_SKIPNOTDATA), so a
   batch's records stay back to back */
#define PM_SCAN_SKIP 2

/* Column arrays for a batch of consecutive records, one element per record.
   The caller allocates the arrays of the columns it wants, leaving the others
   NULL, and sets `capacity` (and `sid_slot_mask` with `sid_index`). Source IDs
   need `sids`, `sid_index`, `sid_slots` and `sid_hashes`; records need `offset`. */
typedef struct PMRecordColumns
{
  int64_t capacity; /* Records each per-record array holds */

  int64_t count;  /* Records scanned */
  int32_t nsids;  /* Distinct source IDs in `sids` */
  int64_t base;   /* Offset in the buffer of the batch's first record */

  int32_t *offset;   /* capacity + 1: start of each record relative to `base`, then the end */
  char *sids;        /* capacity * LM_SIDLEN: distinct source IDs, NUL-terminated */
  int32_t *sid_index; /* Index of each record's source ID in `sids` */

  /* Scratch space for finding source IDs while scanning: a hash table of
     indices into `sids`, with a power-of-two number of slots of at least twice
     `capacity`, and each source ID's hash. Not part of the batch once scanned. */
  int32_t *sid_slots;
  int32_t sid_slot_mask; /* Slots - 1 */
  uint32_t *sid_hashes;  /* capacity */

  int64_t *starttime; /* Nanoseconds since the epoch */
  int64_t *endtime;   /* Nanoseconds since the epoch */
  double *samprate;   /* Hz */
  int64_t *samplecnt;
  int16_t *encoding;
  uint8_t *pubversion;
  uint8_t *formatversion;
} PMRecordColumns;

/* Parse records from `buffer`, starting at `*consumed`, into `cols` until the
   buffer ends, a record continues past it, or `cols` is full. Sets
   `*consumed` to the end of the last record scanned.

   With MSF_SKIPNOTDATA in `flags`, input that isn't a record is skipped a byte
   at a time, as libmseed's reader does, and `*consumed` includes it. A batch
   starts after skipped input and ends before it.

   Returns PM_SCAN_NEEDMORE, PM_SCAN_FULL, PM_SCAN_SKIP, or a negative libmseed
   error code for the record at `*consumed`. */
extern int pm_scan_records (const char *buffer, uint64_t buflen, uint32_t flags, int8_t verbose,
                            PMRecordColumns *cols, uint64_t *consumed);

#endif /* PYMSEED_SCAN_H */
